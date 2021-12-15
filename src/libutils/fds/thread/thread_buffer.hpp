/************************************************************************
Copyright 2021, eBay, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#ifndef MONSTORDB_THREAD_BUFFER_HPP
#define MONSTORDB_THREAD_BUFFER_HPP

#include <memory>
#include <mutex>
#include "libutils/fds/array/flexarray.hpp"
#include <boost/dynamic_bitset.hpp>
#include <libutils/fds/list/sparse_vector.hpp>
#include <common/urcu_helper.hpp>
#include <glog/logging.h>
#include "common/utils.hpp"

namespace fds {

class ThreadRegistry {
#define MAX_RUNNING_THREADS 2048
#define INVALID_CURSOR boost::dynamic_bitset<>::npos

  public:
    ThreadRegistry() :
            m_free_thread_slots(MAX_RUNNING_THREADS),
            m_busy_buf_slots(MAX_RUNNING_THREADS),
            m_bufs_open(MAX_RUNNING_THREADS, 0) {
        // Mark all slots as free
        m_free_thread_slots.set();
        m_slot_cursor = INVALID_CURSOR;
    }

    uint32_t attach() {
        std::lock_guard<std::mutex> lock(m_init_mutex);

        // Wrap around to get the next free slot
        uint32_t thread_num = get_next_free_slot();

        // Mark the slot as not free
        m_free_thread_slots.reset(thread_num);

        char thread_name[256] = {0};
        size_t len = sizeof(thread_name);

#ifdef _POSIX_THREADS
        pthread_getname_np(pthread_self(), thread_name, len);
#endif /* _POSIX_THREADS */
        return thread_num;
    }

    void detach(uint32_t thread_num) {
        m_free_thread_slots.set(thread_num);
        // urcu_ctl::unregister_rcu();
    }

    void inc_buf(uint32_t thread_num) {
        std::lock_guard<std::mutex> lock(m_init_mutex);
        assert(!m_free_thread_slots.test(thread_num));
        m_busy_buf_slots.set(thread_num);
        m_bufs_open[thread_num]++;
    }

    void dec_buf(uint32_t thread_num) {
        std::lock_guard<std::mutex> lock(m_init_mutex);
        do_dec_buf(thread_num);
    }

    void for_all(std::function<void(uint32_t, bool)> cb) {
        m_init_mutex.lock();
        auto i = m_busy_buf_slots.find_first();
        while (i != INVALID_CURSOR) {
            bool thread_exited = m_free_thread_slots.test(i);
            m_init_mutex.unlock();

            cb((uint32_t)i, thread_exited);

            // After callback, if the original thread is indeed freed, free up the slot as well.
            m_init_mutex.lock();
            if (thread_exited) {
                do_dec_buf(i);
            }

            i = m_busy_buf_slots.find_next(i);
        }
        m_init_mutex.unlock();
    }

    static ThreadRegistry* instance() { return &inst; }
    static ThreadRegistry inst;

  private:
    uint32_t get_next_free_slot() {
        do {
            if (m_slot_cursor == INVALID_CURSOR) {
                m_slot_cursor = m_free_thread_slots.find_first();
                if (m_slot_cursor == INVALID_CURSOR) {
                    throw std::invalid_argument("Number of threads exceeded max limit");
                }
            } else {
                m_slot_cursor = m_free_thread_slots.find_next(m_slot_cursor);
            }
        } while ((m_slot_cursor == INVALID_CURSOR) || (m_bufs_open[m_slot_cursor] > 0));

        return (uint32_t)m_slot_cursor;
    }

    void do_dec_buf(uint32_t thread_num) {
        m_bufs_open[thread_num]--;
        if (m_bufs_open[thread_num] == 0) {
            m_busy_buf_slots.reset(thread_num);
        }
    }

  private:
    std::mutex m_init_mutex;

    // A bitset where 1 marks for free thread slot, 0 for not free
    boost::dynamic_bitset<> m_free_thread_slots;

    // Next thread free slot
    boost::dynamic_bitset<>::size_type m_slot_cursor;

    // Number of buffers that are open for a given thread
    boost::dynamic_bitset<> m_busy_buf_slots;
    std::vector<int> m_bufs_open;
};

#define thread_registry ThreadRegistry::instance()

class ThreadLocalContext {
  public:
    ThreadLocalContext() { this_thread_num = thread_registry->attach(); }

    ~ThreadLocalContext() {
        thread_registry->detach(this_thread_num);
        this_thread_num = (uint32_t)-1;
    }

    static ThreadLocalContext* instance() {
        if (inst == nullptr) {
            inst = std::make_unique<ThreadLocalContext>();
        }
        return inst.get();
    }

    static uint32_t my_thread_num() { return instance()->this_thread_num; }
    static void set_context(uint32_t context_id, uint64_t context) { instance()->user_contexts[context_id] = context; }
    static uint64_t get_context(uint32_t context_id) { return instance()->user_contexts[context_id]; }

    static thread_local std::unique_ptr<fds::ThreadLocalContext> inst;

    uint32_t this_thread_num;
    std::array<uint64_t, 5> user_contexts; // To store any user contexts
};

#define THREAD_BUFFER_INIT                                                                                             \
    fds::ThreadRegistry fds::ThreadRegistry::inst;                                                                     \
    thread_local std::unique_ptr<fds::ThreadLocalContext> fds::ThreadLocalContext::inst = nullptr;

template <typename T, typename... Args> class ThreadBuffer {
  public:
    template <class... Args1> ThreadBuffer(Args1&&... args) : m_args(std::forward<Args1>(args)...) {
        m_buffers.reserve(MAX_RUNNING_THREADS);
    }

    T* get() {
        auto tnum = ThreadLocalContext::my_thread_num();
        if (is_new_thread()) {
            std::lock_guard<std::mutex> guard(m_expand_mutex);
            m_buffers[tnum] = std::make_unique<T>(std::forward<Args>(m_args)...);
            thread_registry->inc_buf(tnum);
        }
        return m_buffers[tnum].get();
    }

    T& operator*() { return *(get()); }

    T* operator->() { return get(); }

    T* operator[](uint32_t n) {
        assert(n < get_count());
        return m_buffers[n];
    }

    bool is_new_thread() const {
        auto tnum = ThreadLocalContext::my_thread_num();
        return (!m_buffers.index_exists(tnum) || (m_buffers[tnum].get() == nullptr));
    }

    uint32_t get_count() { return m_buffers.size(); }

    // This method access the buffer for all the threads and do a callback with that thread.
    void access_all_threads(std::function<void(T*)> cb) {
        thread_registry->for_all([this, cb](uint32_t thread_num, bool is_thread_exited) {
            if (m_buffers[thread_num])
                cb(m_buffers[thread_num].get());
        });
    }

    void reset() { m_buffers[ThreadLocalContext::my_thread_num()].reset(); }

  private:
    fds::sparse_vector<std::unique_ptr<T>> m_buffers;
    std::tuple<Args...> m_args;
    std::mutex m_expand_mutex;
};

} // namespace fds
#endif // MONSTORDB_THREAD_BUFFER_HPP
