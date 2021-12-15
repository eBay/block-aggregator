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

#include "common/logging.hpp"
#include <Aggregator/DBConnectionParameters.h>

#include <Client/ConnectionPool.h>

#include <memory>

namespace nuclm {

class LoaderConnectionPool {
  public:
    // ToDo: need to check how to pass in correct cluster name and cluster secret.
    LoaderConnectionPool(const std::string& user_, const std::string& password_, const unsigned max_connections_,
                         const std::string& client_name_, const DatabaseConnectionParameters& conn_parameters_) :
            max_connections(max_connections_),
            client_name(client_name_),
            conn_parameters(conn_parameters_),
            rotating(false),
            active_user(user_),
            active_conn_pool(new DB::ConnectionPool(
                max_connections_, conn_parameters_.host, conn_parameters_.port, conn_parameters_.default_database,
                user_, password_, "", /*cluster, empty, following Client.cpp*/
                "",                   /*cluster_secret, empty, following Client.cpp */
                client_name_, conn_parameters_.compression, conn_parameters_.security)) {}

    // thus we do not shutdown the connections held in the pool when the process exists.
    ~LoaderConnectionPool() = default;

    // the entry's lifetime is controlled by the scope of the returned entry.
    DB::ConnectionPool::Entry get(const DB::ConnectionTimeouts& timeouts, const DB::Settings* settings = nullptr,
                                  bool force_connected = true) {
        return (active_conn_pool.load()->get(timeouts, settings, force_connected));
    }

    bool is_all_conns_free() { return active_conn_pool.load()->isAllFree(); }

    bool is_rotating() { return rotating; }

    std::string get_active_user() {
        std::lock_guard<std::mutex> lock(rotate_mutex);
        return active_user;
    }

    void rotate_credential(const std::string& user_, const std::string& password_) {
        std::lock_guard<std::mutex> lock(rotate_mutex);
        if (rotating.load()) {
            LOG(WARNING) << "Ignore rotation request as it's already rotating to username " << active_user
                         << ", new request username " << user_;
            return;
        }

        if (active_user == user_) {
            LOG(WARNING) << "Ignore rotation request as it's already rotated to username " << active_user;
            return;
        }

        // Establish new connection pool
        LOG(INFO) << "DB credential rotation: connecting with new username " << user_;
        // ToDo: need to check how to pass in correct cluster name and cluster secret.
        DB::ConnectionPool* new_conn_pool = new DB::ConnectionPool(
            max_connections, conn_parameters.host, conn_parameters.port, conn_parameters.default_database, user_,
            password_, "default", "default", client_name, conn_parameters.compression, conn_parameters.security);
        // Try connecting, quit rotation if fail
        try {
            DB::ConnectionPool::Entry entry = new_conn_pool->get(conn_parameters.timeouts, nullptr, true);
        } catch (std::exception& err) {
            LOG(ERROR) << "DB credential rotation: failed to connect with new username " << user_
                       << ", error: " << err.what();
            delete new_conn_pool;
            return;
        }

        // Move active to retire
        retire_conn_pool = active_conn_pool.load();
        retire_user = active_user;

        bool is_all_free = new_conn_pool->isAllFree();
        // Replace active conn pool
        active_conn_pool = new_conn_pool;
        active_user = user_;
        LOG(INFO) << "DB credential rotation: connection pool with new username " << user_
                  << " is activated successfully, all new conns are free: " << std::boolalpha << is_all_free;
        rotating = true;
    }

    /**
     * Delete retired connection pool if it's free.
     * @return false if someone is still using it.
     */
    bool free_retired_conn_pool() {
        std::lock_guard<std::mutex> lock(rotate_mutex);
        if (!rotating.load()) {
            LOG(WARNING) << "Ignore delete retired conn pool request as it's not under rotation process";
            return true;
        }
        if (retire_conn_pool == nullptr) {
            LOG(WARNING) << "Try to delete empty retired conn pool";
            return true;
        }
        if (!retire_conn_pool->isAllFree()) {
            LOG(INFO) << "DB credential rotation: waiting for retired connection pool be free";
            return false;
        }
        delete retire_conn_pool;
        retire_conn_pool = nullptr;
        LOG(INFO) << "DB credential rotation: retired connection pool with old username " << retire_user
                  << " is free and deleted";
        rotating = false;
        return true;
    }

  private:
    // Conn params
    const unsigned max_connections;
    const std::string client_name;
    const DatabaseConnectionParameters& conn_parameters;

    std::mutex rotate_mutex;
    std::atomic<bool> rotating;

    std::string active_user;
    std::atomic<DB::ConnectionPool*> active_conn_pool;

    std::string retire_user;
    DB::ConnectionPool* retire_conn_pool;
};

} // namespace nuclm
