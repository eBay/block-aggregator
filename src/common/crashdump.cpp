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

#include <stdio.h>
#include <iostream>
#include "common/crashdump.hpp"
#include <boost/filesystem.hpp>
#include <iostream>

namespace filesystem = boost::filesystem;

#if defined(BREAKPAD_ON)
#include "common/utils.hpp"
#include <mutex>
#include <boost/filesystem.hpp>
#define CMD_BUFFER_LEN (128)

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif /* __STDC_FORMAT_MACROS */

std::mutex minidump_mutex;
#endif

static std::string get_crash_dir() noexcept {
    const char* value = std::getenv("CRASH_DUMPS_DIR");
    return std::string(value ? value : "crash-dumps");
}

static std::string get_thread_dump_dir() noexcept { return get_crash_dir() + "/thread-dumps"; }

std::string create_crash_dir() noexcept {
    std::string crash_dir = get_crash_dir();
    filesystem::path crash_dir_path{crash_dir};
    if (!filesystem::exists(crash_dir_path) && !filesystem::create_directories(crash_dir_path)) {
        std::cerr << "Unable to create directory " << crash_dir_path << std::endl;
        std::exit(-1);
    }

    filesystem::path thread_dump_dir_path{get_thread_dump_dir()};
    if (!filesystem::exists(thread_dump_dir_path) && !filesystem::create_directories(thread_dump_dir_path)) {
        std::cerr << "Unable to create directory " << thread_dump_dir_path << std::endl;
        std::exit(-1);
    }

    return crash_dir;
}

#if defined(BREAKPAD_ON_APPLE)
// MAC implementation
#include <client/mac/handler/exception_handler.h>
#include <libproc.h>

google_breakpad::ExceptionHandler* eh;

static bool FilterCallback(void* context) {
    (void)context;
    return true;
}

static bool MinidumpCallback(const char* dump_dir, const char* minidump_id, void* context, bool succeeded) {
    (void)context;
    if (succeeded) {
        // print instructions on how access crash dump. The actual dump has already been saved by now
        pid_t pid = getpid();
        char pathbuf[PROC_PIDPATHINFO_MAXSIZE];
        proc_pidpath(pid, pathbuf, sizeof(pathbuf)); // obtain our process's executable path

        // NOTE: while it's tempting to compute the string once and log it to stdout and return here - DONT DO IT!
        // Think out of memory causing a real crash vs on-demand thread dump request when context != nullptr
        if (context != nullptr) {
            std::string* out = (std::string*)context;
            std::stringstream ss;
            ss << dump_dir << '/' << minidump_id << ".dmp";
            *out = ss.str();
            return false; // do not exit!
        }
    }
    return true;
}

std::string enable_minidump() {
    std::lock_guard<std::mutex> l(minidump_mutex);
    if (nullptr == eh) {
        eh = new google_breakpad::ExceptionHandler(get_crash_dir(), FilterCallback, MinidumpCallback, nullptr,
                                                   /*install_handler=*/true, nullptr);
        return "minidump enabled on mac";
    } else {
        return "minidump already enabled on mac";
    }
}

#elif defined(BREAKPAD_ON_LINUX)
// Linux implementation
#include <client/linux/handler/exception_handler.h>

google_breakpad::ExceptionHandler* eh;
google_breakpad::MinidumpDescriptor descriptor(get_crash_dir());

static bool MinidumpCallback(const google_breakpad::MinidumpDescriptor& descriptor, void* context, bool succeeded) {
    if (succeeded) {
        char path[PATH_MAX];
        char pathbuf[PATH_MAX] = {0};
        pid_t pid = getpid();
        snprintf(path, sizeof(path), "/proc/%d/exe", pid);
        if (readlink(path, pathbuf, PATH_MAX) == -1) {
            perror("readlink");
            std::exit(-1);
        }

        if (context != nullptr) {
            std::string* out = (std::string*)context;
            *out = descriptor.path();
            return false;
        }
    }
    return succeeded;
}

std::string enable_minidump() {
    std::lock_guard<std::mutex> l(minidump_mutex);
    if (nullptr == eh) {
        eh = new google_breakpad::ExceptionHandler(descriptor, NULL, MinidumpCallback, NULL,
                                                   /*install_handler=*/true, -1);
        return "minidump enabled on linux";
    } else {
        return "minidump already enabled on linux";
    }
}
#endif

#if defined(BREAKPAD_ON)
std::string disable_minidump() {
    std::lock_guard<std::mutex> l(minidump_mutex);
    if (eh) {
        delete eh;
        eh = nullptr;
        return "minidump disabled";
    } else {
        return "minidump already disabled";
    }
}

std::string generateThreadDump() {
    std::lock_guard<std::mutex> l(minidump_mutex);
    if (!eh) {
        return "minidump is disabled";
    }
    std::string ret;
    eh->WriteMinidump(get_crash_dir(), MinidumpCallback, &ret);
    return ret;
}

std::string getThreadDump() {
    // Generate a mini dump file
    std::lock_guard<std::mutex> l(minidump_mutex);
    if (!eh) {
        return "minidump is disabled";
    }
    std::string ret;
    eh->WriteMinidump(get_thread_dump_dir(), MinidumpCallback, &ret);

    // Get stack info
    // Use a script to translate to readable stack message
    // This implementation is for tess pod env. It won't work in local dev env by default.
    char cmd[1024];
    snprintf(cmd, 1024, "/nucolumnar/bin/thread_dump.sh %s %s", get_crash_dir().c_str(), get_thread_dump_dir().c_str());
    FILE* fp = popen(cmd, "r");
    if (!fp) {
        return "Failed to execute thread_dump.sh";
    }

    std::string stacks;
    std::array<char, CMD_BUFFER_LEN> buffer;
    size_t read_size = 0;
    do {
        read_size = fread(static_cast<void*>(buffer.data()), 1, CMD_BUFFER_LEN, fp);
        if (read_size != 0)
            stacks.append(buffer.data(), read_size);
    } while (read_size == static_cast<size_t>(CMD_BUFFER_LEN));
    pclose(fp);

    return stacks;
}
#else
std::string enable_minidump() { return "minidump is compiling disabled"; }

std::string disable_minidump() { return "minidump is compiling disabled"; }

std::string generateThreadDump() { return "minidump is compiling disabled"; }

std::string getThreadDump() { return "minidump is compiling disabled"; }
#endif
