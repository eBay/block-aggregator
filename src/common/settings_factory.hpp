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
#include <tclap/CmdLine.h>

#include "nucolumnar_aggr_config_generated.h"
#include "common/urcu_helper.hpp"
#include "common/logging.hpp"
#include "common/file_watcher.hpp"
#include <boost/noncopyable.hpp>

using namespace nucolumnar_aggr::config::v1;

namespace nuclm {

struct ImmutableSettings {
    // add immutable settings if needed
};

class NuColumnarSettingsFactory : boost::noncopyable {
    NuColumnarSettingsFactory() {
        cmdline = std::make_unique<TCLAP::CmdLine>("NuColumnar Aggregator", ' ', get_version());
    }

  public:
    static NuColumnarSettingsFactory& instance() {
        static NuColumnarSettingsFactory s_instance;
        return s_instance;
    }
    using SettingListener = std::function<void(NucolumnarAggregatorSettingsT& current, bool to_restart)>;

    static NuColumnarSettingsFactory s_instance;

    // invoke a user callback and supply safely locked settings instance; unlock afterwards
    template <typename CB>
    std::remove_reference_t<std::invoke_result_t<CB, const NucolumnarAggregatorSettingsT&>> with_settings(CB cb) {
        assert(m_rcu_data.get_node() != nullptr);
        auto settings = m_rcu_data.get(); // RAII
        const auto& s = const_cast<const NucolumnarAggregatorSettingsT&>(*settings.get());
        using ret_t = std::invoke_result_t<CB, decltype(s)>;
        static_assert(/*std::is_fundamental_v<ret_t> || */ !std::is_pointer_v<ret_t> && !std::is_reference_v<ret_t>,
                      "Do you really want to return a reference to an object? ");
        return cb(s);
    }

    void load_settings(SettingListener&& cb, bool watch_on_update = false) {
        assert(m_rcu_data.get_node() != nullptr);
        auto settings = m_rcu_data.get();
        cb(*settings.get(), false);
        if (watch_on_update) {
            register_listener(std::move(cb));
        }
    }

    void load(const std::string& configFilePath);

    bool reload();

    /* this function is only used for unit testing */
    void save(const std::string& filepath);

    std::string get_current_settings() const { return m_current_settings; }

    const std::string& get_last_settings_error() const { return m_last_error; }

    std::string get_json() const;

    const std::string get_local_config_file() const { return m_settings_file_path; }

    const std::string& get_version() const;

    static NucolumnarAggregatorSettingsT parse_config(NuColumnarSettingsFactory* factory);

    TCLAP::CmdLine& get_cmdline() { return *cmdline; }

    const ImmutableSettings& immutable_settings() const { return m_i_settings; }

    void register_listener(SettingListener&& listener) { m_settings_listeners.push_back(std::move(listener)); }

    bool exchange_and_notify(NucolumnarAggregatorSettingsT& settings);

    void stop_file_watcher();

  private:
    /* Unparsed settings string */
    std::string m_current_settings;

    /* Last settings parse error is stored in this string */
    std::string m_last_error;

    /* Settings file path */
    std::string m_settings_file_path;

    /* Cmdline instance */
    std::unique_ptr<TCLAP::CmdLine> cmdline;

    /* RCU protected settings data */
    urcu_data<NucolumnarAggregatorSettingsT> m_rcu_data;

    ImmutableSettings m_i_settings;

    std::unique_ptr<FileWatcher> m_file_watcher = nullptr;
    std::vector<SettingListener> m_settings_listeners;
};
} // namespace nuclm

using SETTINGS = const NucolumnarAggregatorSettingsT&;
template <typename CB> inline auto with_settings(CB cb) {
    return ::nuclm::NuColumnarSettingsFactory::instance().with_settings(cb);
}

#define SETTINGS_PARAM(path_expr) with_settings([](SETTINGS s) { return s.path_expr; })

#define SETTINGS_THIS_PARAM(path_expr) with_settings([this](SETTINGS s) { return s.path_expr; })

#define SETTINGS_FACTORY ::nuclm::NuColumnarSettingsFactory::instance()
