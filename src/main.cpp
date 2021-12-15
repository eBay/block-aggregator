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

#include "application.hpp"
#include "common/crashdump.hpp"
#include "common/file_command_flags.hpp"
#include "common/settings_factory.hpp"
#include "common/utils.hpp"
#include "common/backtrace.h"

#include "monitor/metrics_collector.hpp"
#include "http_server/http_server.hpp"

namespace http_server = nuclm::http_server;

#include "libutils/fds/thread/thread_buffer.hpp"
#include <boost/filesystem.hpp>
namespace filesystem = boost::filesystem;

#ifdef USE_CONCURRENT_DS
#include <cds/init.h>
#endif

#include <boost/exception/diagnostic_information.hpp>

std::function<void()> s_graceful_app_shutdown;

void intr_handler(int sig) {
    printf("Received SIGNAL %d\n", sig);
    if (s_graceful_app_shutdown)
        s_graceful_app_shutdown();
    else {
        google::FlushLogFiles(google::GLOG_INFO);
        std::abort();
    }
}

// to handle SIGPIPE due to the broken client-server communication channel with client writing data to the server
void ignore_sigpipe(void) {
    struct sigaction act;
    int r;
    memset(&act, 0, sizeof(act));
    act.sa_handler = SIG_IGN;
    act.sa_flags = SA_RESTART;
    r = sigaction(SIGPIPE, &act, NULL);
    if (r) {
        printf("failed to register signal handler for SIGPIPE\n");
    } else {
        printf("successfully registered signal handler for SIGPIPE\n");
    }
}

struct {
    int sig;
    const char* name;
} signals[] = {{SIGSEGV, "SIGSEGV"}, {SIGABRT, "SIGABRT"}, {SIGBUS, "SIGBUS"}, {SIGILL, "SIGILL"}, {SIGFPE, "SIGFPE"}};
const char* signalName(int sig) {
    for (auto& s : signals)
        if (s.sig == sig)
            return s.name;

    return "unknown";
}
#include <sys/ucontext.h>
// may not used
BOOST_ATTRIBUTE_UNUSED
void segv_handler(int sig, siginfo_t* signal_info, void* ucontext_) {
    ucontext_t* context = (ucontext_t*)ucontext_;
    for (auto s : signals)
        ::signal(s.sig, SIG_DFL);

    printf("received SIGNAL %s:\n", signalName(sig));
    char buff[64 * 1024];
    buff[0] = 0;
    size_t s = stack_backtrace(2, buff, sizeof(buff));
    if (SIGABRT != sig) {
        fprintf(stderr, "stack trace of size %ld bytes\n", s);
        fprintf(stderr, "%s", buff);
    } else {
        LOG(ERROR) << buff;
        google::FlushLogFiles(google::GLOG_INFO);
    }
    ::raise(sig);
}

__attribute__((noinline)) void doSuicide() {
    volatile int* a = (int*)nullptr;

    *a = 1;
}

__attribute__((noinline)) void do_crash() {
    printf("committing suicide ...\n");
    doSuicide();
}
__attribute__((noinline)) void do_abort() {
    printf("performing abort ...\n");
    LOG(FATAL) << " simulated abort";
}

// TODO: As of now we are declaring globals here. Find a cleaner solution for it.
THREAD_BUFFER_INIT;

// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);

RCU_REGISTER_CTL;

template <typename T> static std::string create_desc(const std::string& s, T def) {
    std::stringstream ss;
    ss << s << " (default = " << def << ")";
    return ss.str();
}

#include <common/scope_guard.h>
/// This allows to implement assert to forbid initialization of a class in static constructors.
/// Usage:
///
/// extern bool inside_main;
/// class C { C() { assert(inside_main); } };
bool inside_main = false;

// #if defined(_DEBUG)
extern "C" void __cxa_pure_virtual() {
    while (1)
        ;
}
// #endif

using namespace nuclm;
int main(int argc, const char* const* argv) {
    inside_main = true;
    SCOPE_EXIT({ inside_main = false; });

    // Init minidump
    printf("Enable minidump: %s\n", enable_minidump().c_str());
    printf("Crash dump dir: %s\n", create_crash_dir().c_str());

    // Check command flags directory
    printf("File-based command flags dir: %s\n", PersistentCommandFlags::create_command_flags_dir().c_str());

#ifdef _POSIX_THREADS
#ifdef __APPLE__
    pthread_setname_np("main");
#else
    // commenting this out, because changing the name of the main thread has negative implications on linux. For
    // instance pkill/pgrep stop working
    // pthread_setname_np(pthread_self(), "main");
#endif /* __APPLE__ */
#endif /* _POSIX_THREADS */

    // Only non-optional argument is the config file
    auto cfile_arg = std::make_unique<TCLAP::ValueArg<std::string>>("t", "config-file", "Json config file", false,
                                                                    "/nucolumnar/conf/nucolumnar_aggr_config.json",
                                                                    "string", SETTINGS_FACTORY.get_cmdline());

#define PARAM_ADD(T, var, flag, name, desc, def, type)                                                                 \
    BOOST_ATTRIBUTE_UNUSED auto var = std::make_unique<TCLAP::ValueArg<T>>(                                            \
        flag, name, create_desc<T>(desc, def), false, def, type, SETTINGS_FACTORY.get_cmdline())

    // Optional args follow
    // PARAM_ADD(std::string, coord_arg, "e", "coord-endpoint", "coordinator endpoint in host:port format",
    //           "localhost:8080", "string");
    // PARAM_ADD(std::string, id_arg, "g", "service-group",
    //           "NuColumnar service group name; used for groupping service instances into clusters handling different "
    //           "keyspaces",
    //           "default", "string");
    PARAM_ADD(std::string, log_dir_arg, "l", "log-dir", "logs directory", FLAGS_log_dir, "string");
    // for debugging only
    TCLAP::SwitchArg commit_suicide("k", "suicide", "commit suicide to show the stack trace",
                                    SETTINGS_FACTORY.get_cmdline(), false);
    TCLAP::SwitchArg perform_abort("a", "abort", "execute abort to show the stack trace",
                                   SETTINGS_FACTORY.get_cmdline(), false);
    // parse command line args
    SETTINGS_FACTORY.get_cmdline().parse(argc, argv);

    // config file path
    std::string config_file_path = cfile_arg->getValue();
    if (config_file_path.length() == 0) {
        std::cerr << "Require a json config file" << std::endl << std::flush;
        return 1;
    }

    // log dir path
    std::string log_dir = log_dir_arg->getValue();
    if (log_dir.length() == 0) {
        log_dir = get_cwd_path("logs/");
    }
    FLAGS_log_dir = log_dir;

    if (!filesystem::exists(log_dir) && !filesystem::create_directories(log_dir)) {
        std::cerr << "Failed to create log dir: " << log_dir << std::endl;
        std::exit(-1);
    } else {
        std::cout << "Log Directory: " << log_dir << std::endl;
    }

    // Initialize Google's logging library.
    google::InitGoogleLogging(argv[0]);
    FOREACH_VMODULE(VMODULE_INITIALIZE_MODULE)
    // set log destination
    for (auto i = 0; i < google::NUM_SEVERITIES; i++) {
        std::string logpaths = log_dir + std::string("/NuColumnarAggregator.log.") + google::LogSeverityNames[i] + ".";
        google::SetLogDestination(i, logpaths.c_str());
    }

#if !defined(BREAKPAD_ON)
    struct sigaction sig_action;
    memset(&sig_action, 0, sizeof(sig_action));
    sigemptyset(&sig_action.sa_mask);
    sig_action.sa_flags |= SA_SIGINFO;
    sig_action.sa_sigaction = &segv_handler;
    for (auto s : signals)
        sigaction(s.sig, &sig_action, NULL);
#endif
    LOG(INFO) << "NuColumnar Aggregator version: " << SETTINGS_FACTORY.get_version();

    fds::ThreadLocalContext::set_context(0, 0);

    // for handling ctrl-c, do graceful shutdown in the intr_handler function
    signal(SIGINT, intr_handler);

    // to ignore SIGPIPE explicitly so that the active data sending from aggregator to the clickhouse server will not
    // lead to aggregator process exit when the connection is broken
    ignore_sigpipe();

    // this is just for the purpose of testing of crash-dump writing functionality
    if (commit_suicide.isSet()) {
        do_crash();
    }

    if (perform_abort.isSet()) {
        do_abort();
    }
    int error = 0;

    try {
        SETTINGS_FACTORY.load(config_file_path);
        //---------------------------------------- actual application init logic

        std::mutex m;
        bool stopping = false;
        s_graceful_app_shutdown = [&]() { AppInstance.stop(); };

        LOG(INFO) << "Starting Application";

        // Start application.
        std::condition_variable cv;
        if (!AppInstance.start([&]() {
                {
                    std::unique_lock<std::mutex> lk(m);
                    stopping = true;
                }
                cv.notify_one();
            })) {
            LOG(ERROR) << "Failed to start application!";
        } else {
            LOG(INFO) << "Application is running";

            // Wait until application stops
            std::unique_lock<std::mutex> lk(m);
            cv.wait(lk, [&] { return stopping; });
        }
        s_graceful_app_shutdown = []() {};
        LOG(INFO) << "Application is exiting";
        s_graceful_app_shutdown();
    } catch (std::invalid_argument const& e1) {
        LOG(ERROR) << "Argument error exception (" << e1.what() << ")";
        error = 1;
    } catch (std::bad_alloc const& e2) {
        LOG(ERROR) << "Allocation failed exception (" << e2.what() << ")";
        error = 2;
    } catch (std::exception const& elast) {
        LOG(ERROR) << "Unhandled Exception (" << elast.what() << ")"
                   << ": " << boost::current_exception_diagnostic_information();
        error = 3;
    } catch (...) {
        LOG(ERROR) << boost::current_exception_diagnostic_information();
        error = 4;
    }

    LOG(INFO) << "main() is exiting with code " << error;
    disable_minidump();
    return error;
}
