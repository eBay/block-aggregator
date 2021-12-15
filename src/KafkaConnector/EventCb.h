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

#ifndef NUCOLUMNARAGGR_EVENTCB_H
#define NUCOLUMNARAGGR_EVENTCB_H

#include "common/urcu_helper.hpp"
#include <librdkafka/rdkafkacpp.h>
#include "../common/settings_factory.hpp"

namespace kafka {
class EventHandler : public RdKafka::EventCb {
  public:
    // todo: idx is bad, need to refactor later.
    EventHandler(size_t idx) : stat_{} {
        with_settings([this, idx](SETTINGS s) {
            auto& var = s.config.kafka.configVariants[idx];
            topic_ = var.topic;
            zone_ = var.zone;
        });
    }

    ~EventHandler() = default;

    void event_cb(RdKafka::Event& event) override;

    std::string& get_stat() const;

  private:
    void update_metrics(const std::string& stat_json);

    std::string topic_;
    std::string zone_;
    urcu_data<std::string> stat_;
};
} // namespace kafka
#endif // NUCOLUMNARAGGR_EVENTCB_H
