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

#pragma once
#include <atomic>
#include <cassert>
#include <typeinfo>
#include <iostream>
#include <unordered_map>
#include <string>
#include <cxxabi.h>

namespace fds {

#ifdef _PRERELEASE
using pair_of_atomic_ptrs = std::pair<std::atomic<int64_t>*, std::atomic<int64_t>*>;

class ObjCounterRegistry {
  private:
    std::unordered_map<std::string, pair_of_atomic_ptrs> m_tracker_map;

  public:
    static ObjCounterRegistry& inst() {
        static ObjCounterRegistry instance;
        return instance;
    }

    static decltype(m_tracker_map)& tracker() { return inst().m_tracker_map; }

    static void register_obj(const char* name, pair_of_atomic_ptrs ptrs) { tracker()[std::string(name)] = ptrs; }
};

template <typename T> struct ObjTypeWrapper {
    ObjTypeWrapper(std::atomic<int64_t>* pc, std::atomic<int64_t>* pa) {
        int status;
        char* realname = abi::__cxa_demangle(typeid(T).name(), 0, 0, &status);
        ObjCounterRegistry::register_obj(realname, std::make_pair(pc, pa));
        free(realname);
    }
    int m_dummy; // Dummy value initialized to trigger the registration
};

template <typename T> struct ObjLifeCounter {
    ObjLifeCounter() {
        m_created.fetch_add(1, std::memory_order_seq_cst /* std::memory_order_relaxed*/);
        m_alive.fetch_add(1, std::memory_order_seq_cst /* std::memory_order_relaxed*/);
        m_type.m_dummy = 0; // To keep m_type initialized during compile time
    }

    /*virtual */ ~ObjLifeCounter() {
        assert(m_alive.load() > 0);
        m_alive.fetch_sub(1, std::memory_order_seq_cst /* std::memory_order_relaxed*/);
    }

    ObjLifeCounter(const ObjLifeCounter& o) noexcept {
        m_alive.fetch_add(1, std::memory_order_seq_cst /* std::memory_order_relaxed*/);
    }
    static std::atomic<int64_t> m_created;
    static std::atomic<int64_t> m_alive;
    static ObjTypeWrapper<T> m_type;
};

template <typename T> std::atomic<int64_t> ObjLifeCounter<T>::m_created(0);

template <typename T> std::atomic<int64_t> ObjLifeCounter<T>::m_alive(0);

template <typename T>
ObjTypeWrapper<T> ObjLifeCounter<T>::m_type(&ObjLifeCounter<T>::m_created, &ObjLifeCounter<T>::m_alive);

#else

template <typename T> struct ObjLifeCounter {};
#endif // _PRERELEASE

} // namespace fds
