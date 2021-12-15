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

#ifndef URCU_HELPER_HPP
#define URCU_HELPER_HPP

#include <urcu.h>
//#include <urcu-qsbr.h>

#ifdef _DEBUG
#define ENABLE_LOGGING
#include "common/logging.hpp"
#include <pthread.h>
#include <memory>

#define _log_rcu(msg)                                                                                                  \
    do {                                                                                                               \
        char thread_name[64];                                                                                          \
        pthread_getname_np(pthread_self(), thread_name, sizeof(thread_name));                                          \
        CVLOG(VMODULE_RCU, 4) << thread_name << " :: " << msg;                                                         \
    } while (false)
#else
#define _log_rcu(msg)
#endif

template <typename T> struct urcu_node {
    rcu_head head;
    T val;

    urcu_node(T&& t) : val(std::move(t)) {}

    template <typename... Args> urcu_node(Args&&... args) : val(std::forward<Args>(args)...) {}

    T* get() { return &val; }

    void set(const T& v) { val = v; }
    static void free(struct rcu_head* rh) {
        auto* node = (urcu_node*)rh;
        delete node;
    }
};

/*
 * Internal structure to be used as thread local in urcu_data to automatically register and unregister rcu
 *
 * The main purpose of this structure is to initialize itself whenever new thread arrives in MonstorDB.
 * Note the declaration "static thread local" in urcu_data.
 * */
class urcu_init {
  public:
    urcu_init() {
        _log_rcu("urcu_init()");
        rcu_register_thread();
    }

    ~urcu_init() {
        _log_rcu("~urcu_init()");
        rcu_unregister_thread();
    }
};

class urcu_ctl {
  public:
    static thread_local std::unique_ptr<urcu_init> obj;
    static thread_local bool initialized;

    static void declare_quiescent_state() {
        do_check_initialization();
        rcu_quiescent_state();
    }

    static void urcu_barrier() {
        do_check_initialization();
        _log_rcu("rcu_barrier()");
        rcu_barrier();
    }

    static void do_check_initialization() {
        if (!urcu_ctl::initialized) {
            urcu_ctl::obj = std::make_unique<urcu_init>();
            urcu_ctl::initialized = true;
        }
    }
};

template <typename T> class urcu_ptr {

  public:
    urcu_node<T>* m_gp;

    urcu_ptr(urcu_node<T>* gp) : m_gp(gp) {
        urcu_ctl::do_check_initialization();
        _log_rcu("rcu_read_lock()");
        rcu_read_lock();
    }
    ~urcu_ptr() {
        urcu_ctl::do_check_initialization();
        _log_rcu("rcu_read_unlock()");
        rcu_read_unlock();
    }

#if 0
    urcu_ptr(urcu_ptr&& other) noexcept {
        urcu_ctl::do_check_initialization();
        _log_rcu("rcu_read_lock()");
        rcu_read_lock();
        m_gp = std::move(other.m_gp);
    }
#endif

    T* operator->() const {
        urcu_ctl::do_check_initialization();
        auto node = rcu_dereference(m_gp);
        return &node->val;
    }

    T* get() const {
        urcu_ctl::do_check_initialization();
        auto node = rcu_dereference(m_gp);
        return &node->val;
    }

  private:
    urcu_ptr(const urcu_ptr& other) = delete;
    urcu_ptr& operator=(const urcu_ptr& other) = delete;
    urcu_ptr(urcu_ptr&& other) = delete;
    urcu_ptr& operator=(urcu_ptr&& other) = delete;
};

class urcu_lock {
  public:
    urcu_lock() {
        urcu_ctl::do_check_initialization();
        _log_rcu("rcu_read_lock()");
        rcu_read_lock();
    }
    ~urcu_lock() {
        urcu_ctl::do_check_initialization();
        _log_rcu("rcu_read_unlock()");
        rcu_read_unlock();
    }

  private:
    urcu_lock(const urcu_lock& other) = delete;
    urcu_lock& operator=(const urcu_lock& other) = delete;
    urcu_lock(urcu_lock&& other) = delete;
    urcu_lock& operator=(urcu_lock&& other) = delete;
};

template <typename T> class urcu_data {
  public:
    template <typename... Args> urcu_data(Args&&... args) {
        urcu_ctl::do_check_initialization();
        auto node = new urcu_node<T>(std::forward<Args>(args)...);
        rcu_assign_pointer(m_rcu_node, node);
    }

    ~urcu_data() { delete (m_rcu_node); }

    template <typename... Args> urcu_node<T>* make_and_exchange(Args&&... args) {
        urcu_ctl::do_check_initialization();
        auto new_node = new urcu_node<T>(std::forward<Args>(args)...);
        auto old_rcu_node = get_node();
        rcu_assign_pointer(m_rcu_node, new_node);
        call_rcu(&old_rcu_node->head, urcu_node<T>::free);
        return old_rcu_node;
    }

    /* Move the precreated object T into urcu_node */
    urcu_node<T>* move_and_exchange(T& t) { return exchange(std::move(t)); }

    urcu_node<T>* exchange(T&& t) {
        urcu_ctl::do_check_initialization();
        auto new_node = new urcu_node<T>(std::move(t));
        auto old_rcu_node = get_node();
        rcu_assign_pointer(m_rcu_node, new_node);
        call_rcu(&old_rcu_node->head, urcu_node<T>::free);
        return old_rcu_node;
    }

    urcu_node<T>* replace(urcu_node<T>* new_node) {
        auto old_rcu_node = get_node();
        rcu_assign_pointer(m_rcu_node, new_node);
        call_rcu(&old_rcu_node->head, urcu_node<T>::free);
        return old_rcu_node;
    }

    urcu_ptr<T> get() const {
        assert(m_rcu_node != nullptr);
        urcu_ctl::do_check_initialization();
        return urcu_ptr<T>(m_rcu_node);
    }

    urcu_node<T>* get_node() const {
        urcu_ctl::do_check_initialization();
        return m_rcu_node;
    }

  private:
    urcu_node<T>* m_rcu_node;
};

#define RCU_REGISTER_CTL                                                                                               \
    thread_local std::unique_ptr<urcu_init> urcu_ctl::obj = nullptr;                                                   \
    thread_local bool urcu_ctl::initialized;

#endif // URCU_HELPER_HPP
