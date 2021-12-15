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
/*
 * Logging: We use GLog for logging the data. We customized the Google GLog, so that it can accept the
 * user provided module name instead of defaulting always to a filename. This way we can logically split
 * the module, that cut across multiple files.
 *
 * In order to log a message, suggested approach is
 *
 * DCVLOG(<module_name>, <verbose_level>) << "Your log message if built in debug mode";
 * CVLOG(<module_name>, <verbose_level>) << "Your debug and release log message";
 * LOG(ERROR|WARNING|FATAL|INFO) << "Your unconditional log message";
 * DLOG(ERROR|WARNING|FATAL|INFO) << "Your unconditional log message only in debug";
 *
 * LOG(DFATAL) << "Your message and assert in Debug mode, treats as LOG(ERROR) in release mode".
 *
 * Please avoid using DVLOG and VLOG directly as it puts module name as filename, which violates the general
 * purpose nature of this. Please generously use LOG(DFATAL) instead of assert, since in production release
 * it can log message instead of silently discarding it (unless the assert check will affect performance)
 *
 * Please pick a module name listed below. If it does not suit you, add one and use that name while logging.
 * Suggested verbose level is 1 - 6. While having a verbose level per module, gives the flexibility to use
 * different verbose standards for different modules, in production, its always convenient to stick to
 * similar verbosity across modules as well.
 *
 * How to enable logging:
 * Set environment variables
 *  GLOG_v=<common verbose level>
 *  GLOG_vmodule="<module name1>=<overridden verbose level>,<module name2>=<overridden verbose level>"
 *
 *  Example: GLOG_v=0, GLOG_vmodule="txn=5,op=4,network=1" ...
 *
 * How to set the level dynamically:
 * TODO: Work in progress...
 *
 */
#include <glog/logging.h>

#define VMODULE_ADMIN admin
#define VMODULE_APP app
#define VMODULE_HTTP_SERVER http_server
#define VMODULE_METRICS metrics
#define VMODULE_RCU rcu
#define VMODULE_SETTINGS settings
#define VMODULE_KAFKA_CONSUMER kafka_consumer
#define VMODULE_AGGR_PROCESSOR aggr_processor
#define VMODULE_AGGR_MESSAGE aggr_message

// NOTE: If new modules are introduced, add it into above #define and also remember to add to the list below.
#define FOREACH_VMODULE(method)                                                                                        \
    method(VMODULE_ADMIN) method(VMODULE_APP) method(VMODULE_HTTP_SERVER) method(VMODULE_METRICS) method(VMODULE_RCU)  \
        method(VMODULE_SETTINGS) method(VMODULE_KAFKA_CONSUMER) method(VMODULE_AGGR_PROCESSOR)                         \
            method(VMODULE_AGGR_MESSAGE)

#define VMODULE_STR_INTERNAL(m) #m
#define VMODULE_STR(m) VMODULE_STR_INTERNAL(m)
#define VMODULE_LIST_STR(m) VMODULE_STR_INTERNAL(m),
#define VMODULE_REGISTER_MODULE(m) VLOG_REG_MODULE(m);
#define VMODULE_DECLARE_MODULE(m) VLOG_DECL_MODULE(m);
#define VMODULE_INITIALIZE_MODULE(m)                                                                                   \
    if (google::GetVLOGLevel(VMODULE_STR(m)) == -1) {                                                                  \
        google::SetVLOGLevel(VMODULE_STR(m), getenv("GLOG_v") ? atoi(getenv("GLOG_v")) : 0);                           \
    }

//#define VMODULE_INITIALIZE_MODULE(m) VMODULE_INITIALIZE_MODULE_INTERNAL(m)

#define VMODULE_ALL_LIST FOREACH_VMODULE(VMODULE_LIST_STR)

// Register all modules with Glog subsystem. This creates the global variable for each module
FOREACH_VMODULE(VMODULE_REGISTER_MODULE);

#define CVLOG_M(custom_module, verboselevel)                                                                           \
    LOG_IF(INFO, CVLOG_IS_ON(custom_module, verboselevel)) << "[" << VMODULE_STR_INTERNAL(custom_module) << "]"

#define CVLOG_MC(custom_module, component, verboselevel)                                                               \
    LOG_IF(INFO, CVLOG_IS_ON(custom_module, verboselevel))                                                             \
        << "[" << VMODULE_STR_INTERNAL(custom_module) << "::" << VMODULE_STR_INTERNAL(component) << "]"                \
        << "===>"

#define CVLOG_MC_WARN(custom_module, component)                                                                        \
    LOG(WARNING) << "[" << VMODULE_STR_INTERNAL(custom_module) << "::" << VMODULE_STR_INTERNAL(component) << "]"       \
                 << "===>"

#define CVLOG_MC_ERR(custom_module, component)                                                                         \
    LOG(ERROR) << "[" << VMODULE_STR_INTERNAL(custom_module) << "::" << VMODULE_STR_INTERNAL(component) << "]"         \
               << "===>"

#define _Log(level) CVLOG_MC(LOG_MODULE, LOG_COMPONENT, level)
#define _LogE CVLOG_MC_ERR(LOG_MODULE, LOG_COMPONENT)
#define _LogW CVLOG_MC_WARN(LOG_MODULE, LOG_COMPONENT)

#define LOG_ADMIN(level) CVLOG(VMODULE_ADMIN, level)
#define LOG_APP(level) CVLOG(VMODULE_APP, level)
#define LOG_CMD(level) CVLOG(VMODULE_CMD, level)
#define LOG_HTTP(level) CVLOG(VMODULE_HTTP, level)
#define LOG_KAFKA(level) CVLOG(VMODULE_KAFKA_CONSUMER, level)
#define LOG_RPC(level) CVLOG(VMODULE_RPC, level)
#define LOG_SETTINGS(level) CVLOG(VMODULE_SETTINGS, level)
#define LOG_AGGRPROC(level) CVLOG(VMODULE_AGGR_PROCESSOR, level)
#define LOG_MESSAGE(level) CVLOG(VMODULE_AGGR_MESSAGE, level)
