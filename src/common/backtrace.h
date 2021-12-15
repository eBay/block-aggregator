/**
 * Copyright (C) 2017-present Jung-Sang Ahn <jungsang.ahn@gmail.com>
 * All rights reserved.
 *
 * https://github.com/greensky00
 *
 * Stack Backtrace
 * Version: 0.3.5
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#pragma once

// LCOV_EXCL_START

#define SIZE_T_UNUSED size_t __attribute__((unused))
#define VOID_UNUSED void __attribute__((unused))
#define UINT64_T_UNUSED uint64_t __attribute__((unused))
#define STR_UNUSED std::string __attribute__((unused))
#define INTPTR_UNUSED intptr_t __attribute__((unused))

#include <cstddef>
#include <sstream>
#include <string>

#include <cxxabi.h>
#include <execinfo.h>

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif /* __STDC_FORMAT_MACROS */
#include <inttypes.h>

#include <stdio.h>
#include <signal.h>

#ifdef __APPLE__
#include <mach-o/getsect.h>
#include <mach-o/dyld.h>

static UINT64_T_UNUSED static_base_address(void) {
    const struct segment_command_64* command = getsegbyname(SEG_TEXT /*"__TEXT"*/);
    uint64_t addr = command->vmaddr;
    return addr;
}

static STR_UNUSED get_exec_path() {
    char path[1024];
    uint32_t size = sizeof(path);
    if (_NSGetExecutablePath(path, &size) != 0)
        return std::string();

    return path;
}

static STR_UNUSED get_file_part(const std::string& full_path) {
    size_t pos = full_path.rfind("/");
    if (pos == std::string::npos)
        return full_path;

    return full_path.substr(pos + 1, full_path.size() - pos - 1);
}

static INTPTR_UNUSED image_slide(void) {
    std::string exec_path = get_exec_path();
    if (exec_path.empty())
        return -1;

    auto image_count = _dyld_image_count();
    for (decltype(image_count) i = 0; i < image_count; i++) {
        if (strcmp(_dyld_get_image_name(i), exec_path.c_str()) == 0) {
            return _dyld_get_image_vmaddr_slide(i);
        }
    }
    return -1;
}
#endif

#define _snprintf(msg, avail_len, cur_len, msg_len, args...)                                                           \
    avail_len = (avail_len > cur_len) ? (avail_len - cur_len) : 0;                                                     \
    msg_len = snprintf(msg + cur_len, avail_len, args);                                                                \
    cur_len += (avail_len > msg_len) ? msg_len : avail_len

static SIZE_T_UNUSED _stack_backtrace(void** stack_ptr, size_t stack_ptr_capacity) {
    return backtrace(stack_ptr, stack_ptr_capacity);
}

static SIZE_T_UNUSED _stack_interpret_linux(size_t skip_frames, void* const* stack_ptr, char* const* stack_msg,
                                            int stack_size, char* output_buf, size_t output_buflen);

static SIZE_T_UNUSED _stack_interpret_apple(size_t skip_frames, void* const* stack_ptr, char* const* stack_msg,
                                            int stack_size, char* output_buf, size_t output_buflen);

static SIZE_T_UNUSED _stack_interpret_other(size_t skip_frames, void* const* stack_ptr, char* const* stack_msg,
                                            int stack_size, char* output_buf, size_t output_buflen);

static SIZE_T_UNUSED _stack_interpret(size_t skip_frames, void* const* stack_ptr, int stack_size, char* output_buf,
                                      size_t output_buflen) {
    char** stack_msg = nullptr;
    stack_msg = backtrace_symbols(stack_ptr, stack_size);

    size_t len = 0;

#if defined(__linux__)
    len = _stack_interpret_linux(skip_frames, stack_ptr, stack_msg, stack_size, output_buf, output_buflen);

#elif defined(__APPLE__)
    len = _stack_interpret_apple(skip_frames, stack_ptr, stack_msg, stack_size, output_buf, output_buflen);

#else
    len = _stack_interpret_other(skip_frames, stack_ptr, stack_msg, stack_size, output_buf, output_buflen);

#endif
    free(stack_msg);

    return len;
}

static SIZE_T_UNUSED _stack_interpret_linux(size_t skip_frames, void* const* stack_ptr, char* const* stack_msg,
                                            int stack_size, char* output_buf, size_t output_buflen) {
    size_t cur_len = 0;
#ifdef __linux__
    size_t frame_num = 0;

    // NOTE: add 1 to skip this frame.
    for (int i = skip_frames + 1; i < stack_size; ++i) {
        // `stack_msg[x]` format:
        //   /foo/bar/executable() [0xabcdef]
        //   /lib/x86_64-linux-gnu/libc.so.6(__libc_start_main+0xf0) [0x123456]

        // NOTE: with ASLR
        //   /foo/bar/executable(+0x5996) [0x555555559996]

        int fname_len = 0;
        while (stack_msg[i][fname_len] != '(' && stack_msg[i][fname_len] != ' ' && stack_msg[i][fname_len] != 0x0) {
            ++fname_len;
        }

        char addr_str[256];
        uintptr_t actual_addr = 0x0;
        if (stack_msg[i][fname_len] == '(' && stack_msg[i][fname_len + 1] == '+') {
            // ASLR is enabled, get the offset from here.
            int upto = fname_len + 2;
            while (stack_msg[i][upto] != ')' && stack_msg[i][upto] != 0x0) {
                upto++;
            }
            sprintf(addr_str, "%.*s", upto - fname_len - 2, &stack_msg[i][fname_len + 2]);

            // Convert hex string -> integer address.
            std::stringstream ss;
            ss << std::hex << addr_str;
            ss >> actual_addr;

        } else {
            actual_addr = (uintptr_t)stack_ptr[i];
            sprintf(addr_str, "%" PRIxPTR, actual_addr);
        }

        char cmd[1024];
        snprintf(cmd, 1024, "addr2line -f -e %.*s %s", fname_len, stack_msg[i], addr_str);
        FILE* fp = popen(cmd, "r");
        if (!fp)
            continue;

        char mangled_name[1024];
        char file_line[1024];
        int ret = fscanf(fp, "%1023s %1023s", mangled_name, file_line);
        (void)ret;
        pclose(fp);

        size_t msg_len = 0;
        size_t avail_len = output_buflen;
        _snprintf(output_buf, avail_len, cur_len, msg_len, "#%-2zu 0x%016" PRIxPTR " in ", frame_num++, actual_addr);

        int status;
        char* cc = abi::__cxa_demangle(mangled_name, 0, 0, &status);
        if (cc) {
            _snprintf(output_buf, avail_len, cur_len, msg_len, "%s at ", cc);
        } else {
            std::string msg_str = stack_msg[i];
            std::string _func_name = msg_str;
            size_t s_pos = msg_str.find("(");
            size_t e_pos = msg_str.rfind("+");
            if (e_pos == std::string::npos)
                e_pos = msg_str.rfind(")");
            if (s_pos != std::string::npos && e_pos != std::string::npos) {
                _func_name = msg_str.substr(s_pos + 1, e_pos - s_pos - 1);
            }
            _snprintf(output_buf, avail_len, cur_len, msg_len, "%s() at ",
                      (_func_name.empty() ? mangled_name : _func_name.c_str()));
        }

        _snprintf(output_buf, avail_len, cur_len, msg_len, "%s\n", file_line);
    }

#endif
    return cur_len;
}

static VOID_UNUSED skip_whitespace(const std::string base_str, size_t& cursor) {
    while (base_str[cursor] == ' ')
        cursor++;
}

static VOID_UNUSED skip_until_whitespace(const std::string base_str, size_t& cursor) {
    while (base_str[cursor] != ' ')
        cursor++;
}

static SIZE_T_UNUSED _stack_interpret_apple(size_t skip_frames, void* const* stack_ptr, char* const* stack_msg,
                                            int stack_size, char* output_buf, size_t output_buflen) {
    size_t cur_len = 0;
#ifdef __APPLE__

    size_t frame_num = 0;
    (void)frame_num;

    std::string exec_full_path = get_exec_path();
    std::string exec_file = get_file_part(exec_full_path);
    uint64_t load_base = (uint64_t)image_slide() + static_base_address();
    std::stringstream ss;
    ss << "atos -l 0x";
    ss << std::hex << load_base;
    ss << " -o " << exec_full_path;

    // FIRST PASS: build atos command line string only
    // NOTE: add 1 to skip this frame.
    for (int i = skip_frames + 1; i < stack_size; ++i) {
        // `stack_msg[x]` format:
        //   8   foobar    0x000000010fd490da main + 1322
        if (!stack_msg[i] || stack_msg[i][0] == 0x0)
            continue;

        std::string base_str = stack_msg[i];

        size_t s_pos = 0;
        size_t len = 0;
        size_t cursor = 0;

        // Skip frame number part.
        skip_until_whitespace(base_str, cursor);

        // Skip whitespace.
        skip_whitespace(base_str, cursor);
        s_pos = cursor;
        // Filename part.
        skip_until_whitespace(base_str, cursor);
        len = cursor - s_pos;
        std::string filename = base_str.substr(s_pos, len);

        // Skip whitespace.
        skip_whitespace(base_str, cursor);
        s_pos = cursor;
        // Address part.
        skip_until_whitespace(base_str, cursor);
        len = cursor - s_pos;
        std::string address = base_str.substr(s_pos, len);
        if (!address.empty() && address[0] == '?')
            continue;

        ss << " " << address;
    }
    auto atos = ss.str();
    printf("executing %s\n", atos.c_str());
    FILE* fp = popen(atos.c_str(), "r");
    // SECOND PASS: repeat the same steps as in FIRST PASS but this time taken into account atos output for each frame
    // NOTE: add 1 to skip this frame.
    for (int i = skip_frames + 1; i < stack_size; ++i) {
        // `stack_msg[x]` format:
        //   8   foobar    0x000000010fd490da main + 1322
        if (!stack_msg[i] || stack_msg[i][0] == 0x0)
            continue;

        std::string base_str = stack_msg[i];

        size_t s_pos = 0;
        size_t len = 0;
        size_t cursor = 0;

        // Skip frame number part.
        skip_until_whitespace(base_str, cursor);

        // Skip whitespace.
        skip_whitespace(base_str, cursor);
        s_pos = cursor;
        // Filename part.
        skip_until_whitespace(base_str, cursor);
        len = cursor - s_pos;
        std::string filename = base_str.substr(s_pos, len);

        // Skip whitespace.
        skip_whitespace(base_str, cursor);
        s_pos = cursor;
        // Address part.
        skip_until_whitespace(base_str, cursor);
        len = cursor - s_pos;
        std::string address = base_str.substr(s_pos, len);
        if (!address.empty() && address[0] == '?')
            continue;

        // Skip whitespace.
        skip_whitespace(base_str, cursor);
        s_pos = cursor;
        // Mangled function name part.
        skip_until_whitespace(base_str, cursor);
        len = cursor - s_pos;
        std::string func_mangled = base_str.substr(s_pos, len);

        size_t msg_len = 0;
        size_t avail_len = output_buflen;

        _snprintf(output_buf, avail_len, cur_len, msg_len, "#%-2zu %s in ", frame_num++, address.c_str());

        char atos_cstr[4096];
        if (fp != nullptr && fgets(atos_cstr, 4095, fp)) {
            std::string atos_str = atos_cstr;
            size_t d_pos = atos_str.find(" (in ");
            if (d_pos != std::string::npos) {
                _snprintf(output_buf, avail_len, cur_len, msg_len, "%s", atos_cstr);
                continue;
                // std::string function_part = atos_str.substr(0, d_pos);
                // d_pos = atos_str.find(") (", d_pos);
                // if (d_pos != std::string::npos) {
                //     std::string source_part = atos_str.substr(d_pos + 3);
                //     source_part = source_part.substr(0, source_part.size() - 2);

                //     _snprintf(output_buf, avail_len, cur_len, msg_len, "%s in %s\n", function_part.c_str(),
                //               source_part.c_str());
                //     continue;
                // }
            }
        }

        // Dynamic library or static library.
        int status;
        char* cc = abi::__cxa_demangle(func_mangled.c_str(), 0, 0, &status);
        if (cc) {
            _snprintf(output_buf, avail_len, cur_len, msg_len, "%s at %s\n", cc, filename.c_str());
        } else {
            _snprintf(output_buf, avail_len, cur_len, msg_len, "%s() at %s\n", func_mangled.c_str(), filename.c_str());
        }
    }
#endif
    return cur_len;
}

static SIZE_T_UNUSED _stack_interpret_other(size_t skip_frames, void* const* stack_ptr, char* const* stack_msg,
                                            int stack_size, char* output_buf, size_t output_buflen) {
    size_t cur_len = 0;
    size_t frame_num = 0;
    (void)frame_num;

    // NOTE: add 1 to skip this frame.
    for (int i = skip_frames + 1; i < stack_size; ++i) {
        // On non-Linux platform, just use the raw symbols.
        size_t msg_len = 0;
        size_t avail_len = output_buflen;
        _snprintf(output_buf, avail_len, cur_len, msg_len, "%s\n", stack_msg[i]);
    }
    return cur_len;
}

static SIZE_T_UNUSED stack_backtrace(size_t skip_frames, char* output_buf, size_t output_buflen) {
    void* stack_ptr[256];
    int stack_size = _stack_backtrace(stack_ptr, 256);
    return _stack_interpret(skip_frames, stack_ptr, stack_size, output_buf, output_buflen);
}

// LCOV_EXCL_STOP
