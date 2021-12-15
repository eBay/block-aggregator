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

#include <common/utils.hpp>
#include <common/logging.hpp>

#include <sys/time.h>
#include <sys/timeb.h>

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#else
#include <jemalloc/jemalloc.h>
#endif
#include <cstring>
#include <cstdio>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>

//---URL ENCODING/DECODING---
// Converts a hex character to its integer value
char from_hex(char ch) { return isdigit(ch) ? ch - '0' : tolower(ch) - 'a' + 10; }

static char hexmap[] = "0123456789abcdef";

// Converts an integer value to its hex character
char to_hex(char code) { return hexmap[code & 15]; }

// Returns a url-encoded version of str
// IMPORTANT: be sure to free() the returned string after use
char* url_encode(char* str) {
    char *pstr = str, *buf = (char*)malloc(std::strlen(str) * 3 + 1), *pbuf = buf;
    while (*pstr) {
        if (isalnum(*pstr) || *pstr == '-' || *pstr == '_' || *pstr == '.' || *pstr == '~')
            *pbuf++ = *pstr;
        else if (*pstr == ' ')
            *pbuf++ = '+';
        else
            *pbuf++ = '%', *pbuf++ = to_hex(*pstr >> 4), *pbuf++ = to_hex(*pstr & 15);
        pstr++;
    }
    *pbuf = '\0';
    return buf;
}

// Returns a url-decoded version of str : DECODING is done in  place
char* url_decode(char* str) {
    char *p_in = str, *p_out = str;
    while (*p_in) {
        if (*p_in == '%') {
            if (p_in[1] && p_in[2]) {
                *p_out++ = from_hex(p_in[1]) << 4 | from_hex(p_in[2]);
                p_in += 3;
            }
        } else if (*p_in == '+') {
            *p_out++ = ' ';
            p_in++;
        } else {
            *p_out++ = *p_in++;
        }
    }
    *p_out = '\0';
    return str;
}

std::string bin2hex(unsigned char* data, int len) {
    std::string s(len * 2, ' ');
    for (int i = 0; i < len; ++i) {
        s[2 * i] = hexmap[(data[i] & 0xF0) >> 4];
        s[2 * i + 1] = hexmap[data[i] & 0x0F];
    }
    return s;
}
#include <openssl/md5.h>

std::string md5_hex(const void* data, size_t len) {
    unsigned char d[MD5_DIGEST_LENGTH];
    MD5((const unsigned char*)data, len, d);
    return bin2hex(d, sizeof(d));
}

#include <fstream>
#include <streambuf>

std::string load_file(std::ifstream& file) {
    std::string str;

    file.seekg(0, std::ios::end);
    str.reserve(file.tellg());
    file.seekg(0, std::ios::beg);

    str.assign((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    return str;
}

std::string load_file(const char* file_name) {
    std::ifstream file(file_name);
    return load_file(file);
}

char* json_escape(const char* str, int len) {
    int pos = 0, pos2 = 0, new_length = 0;
    unsigned char c;

    for (pos = 0; pos < len; pos++) {
        c = str[pos];
        switch (c) {
        case '\b':
        case '\n':
        case '\r':
        case '\t':
        case '"':
        case '\\':
        case '/':
            new_length++;
        }
        new_length++;
    }

    char* return_str = (char*)malloc((new_length + 1) * sizeof(char));

    for (pos = 0; pos < len; pos++) {
        c = str[pos];

        switch (c) {
        case '\b':
            return_str[pos2++] = '\\';
            return_str[pos2++] = 'b';
            break;
        case '\n':
            return_str[pos2++] = '\\';
            return_str[pos2++] = 'n';
            break;
        case '\r':
            return_str[pos2++] = '\\';
            return_str[pos2++] = 'r';
            break;
        case '\t':
            return_str[pos2++] = '\\';
            return_str[pos2++] = 't';
            break;
        case '"':
            return_str[pos2++] = '\\';
            return_str[pos2++] = '\"';
            break;
        case '\\':
            return_str[pos2++] = '\\';
            return_str[pos2++] = '\\';
            break;
        case '/':
            return_str[pos2++] = '\\';
            return_str[pos2++] = '/';
            break;
        default:
            return_str[pos2++] = c;
        }
    }

    return_str[pos2] = '\0';
    return return_str;
}
//---URL ENCODING/DECODING END---

uint64_t g_get_nano_time(void) {
    struct timespec tp;

#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
    clock_serv_t cclock;
    mach_timespec_t mts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);
    tp.tv_sec = mts.tv_sec;
    tp.tv_nsec = mts.tv_nsec;

#else
    clock_gettime(CLOCK_MONOTONIC, &tp);
#endif
    return (((uint64_t)tp.tv_sec) * 1000000000) + tp.tv_nsec;
}

#define _MIN_READ(a, b) ((a) < (b) ? (a) : (b))

#ifndef HOST_BIG_ENDIAN
/* Little-endian cmp macros */
#define _str3_cmp(m, c0, c1, c2, c3) *(uint32_t*)m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0)

#define _str3Ocmp(m, c0, c1, c2, c3) *(uint32_t*)m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0)

#define _str4cmp(m, c0, c1, c2, c3) *(uint32_t*)m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0)

#define _str5cmp(m, c0, c1, c2, c3, c4) *(uint32_t*)m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0) && m[4] == c4

#define _str6cmp(m, c0, c1, c2, c3, c4, c5)                                                                            \
    *(uint32_t*)m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0) && (((uint32_t*)m)[1] & 0xffff) == ((c5 << 8) | c4)

#define _str7_cmp(m, c0, c1, c2, c3, c4, c5, c6, c7)                                                                   \
    *(uint32_t*)m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0) &&                                                     \
        ((uint32_t*)m)[1] == ((c7 << 24) | (c6 << 16) | (c5 << 8) | c4)

#define _str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7)                                                                    \
    *(uint32_t*)m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0) &&                                                     \
        ((uint32_t*)m)[1] == ((c7 << 24) | (c6 << 16) | (c5 << 8) | c4)

#define _str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8)                                                                \
    *(uint32_t*)m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0) &&                                                     \
        ((uint32_t*)m)[1] == ((c7 << 24) | (c6 << 16) | (c5 << 8) | c4) && m[8] == c8
#else
/* Big endian cmp macros */
#define _str3_cmp(m, c0, c1, c2, c3) m[0] == c0&& m[1] == c1&& m[2] == c2

#define _str3Ocmp(m, c0, c1, c2, c3) m[0] == c0&& m[2] == c2&& m[3] == c3

#define _str4cmp(m, c0, c1, c2, c3) m[0] == c0&& m[1] == c1&& m[2] == c2&& m[3] == c3

#define _str5cmp(m, c0, c1, c2, c3, c4) m[0] == c0&& m[1] == c1&& m[2] == c2&& m[3] == c3&& m[4] == c4

#define _str6cmp(m, c0, c1, c2, c3, c4, c5) m[0] == c0&& m[1] == c1&& m[2] == c2&& m[3] == c3&& m[4] == c4&& m[5] == c5

#define _str7_cmp(m, c0, c1, c2, c3, c4, c5, c6, c7)                                                                   \
    m[0] == c0&& m[1] == c1&& m[2] == c2&& m[3] == c3&& m[4] == c4&& m[5] == c5&& m[6] == c6

#define _str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7)                                                                    \
    m[0] == c0&& m[1] == c1&& m[2] == c2&& m[3] == c3&& m[4] == c4&& m[5] == c5&& m[6] == c6&& m[7] == c7

#define _str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8)                                                                \
    m[0] == c0&& m[1] == c1&& m[2] == c2&& m[3] == c3&& m[4] == c4&& m[5] == c5&& m[6] == c6&& m[7] == c7&& m[8] == c8

#endif

#include <nucolumnar_aggr_config_generated.h>

const std::string& load_settings_schema() {
    static std::string loaded_schema = nucolumnar_aggr_config_fbs;
    return loaded_schema;
}

size_t get_jemalloc_dirty_page_count() {
    const char* arena_dirty_prefix = "stats.arenas.";
    const char* arena_dirty_sufix = ".pdirty";
    size_t npages = 0;
    size_t szu = sizeof(unsigned int);
    unsigned int ua;
#ifdef __MACH__
    // return 0 for MAC, may add MAC specific code here.
#elif defined(USE_LIB_JEMALLOC)
    if (mallctl("arenas.narenas", &ua, &szu, NULL, 0) == 0) {
        for (unsigned int i = 0; i < ua; i++) {
            char arena_index[11];
            sprintf(arena_index, "%d", i);
            size_t index_length = strlen(arena_index);
            char arena_dirty_page_name[21 + index_length];
            memcpy(arena_dirty_page_name, arena_dirty_prefix, 13);
            memcpy(arena_dirty_page_name + 13, arena_index, index_length);
            memcpy(arena_dirty_page_name + 13 + index_length, arena_dirty_sufix, 8);

            size_t sz = sizeof(size_t);
            size_t arena_dirty_page = 0;
            if (mallctl(arena_dirty_page_name, &arena_dirty_page, &sz, NULL, 0) == 0) {
                npages += arena_dirty_page;
            }
        }
    } else {
        LOG(WARNING) << "fail to get the number of arenas from jemalloc";
    }
#endif
    return npages;
}

/* Get the MonstorDB total allocated memory. Relies on jemalloc. Returns 0 for MAC now. */
size_t get_total_memory(bool refresh) {
    size_t allocated = 0;
    size_t sz_allocated = sizeof(allocated);
#ifdef __MACH__
    // return 0 for MAC, may add MAC specific code here.
#elif defined(USE_LIB_JEMALLOC)
    if (refresh) {
        uint64_t epoch = 1;
        size_t sz_epoch = sizeof(epoch);
        if (mallctl("epoch", &epoch, &sz_epoch, &epoch, sz_epoch) != 0) {
            LOG(WARNING) << "fail to refresh jemalloc memory usage stats";
        }

        if (mallctl("stats.allocated", &allocated, &sz_allocated, NULL, 0) != 0) {
            allocated = 0;
        }
        size_t mapped = 0;
        if (mallctl("stats.mapped", &mapped, &sz_allocated, NULL, 0) != 0) {
            mapped = 0;
        }
        CVLOG(VMODULE_ADMIN, 5) << "Allocated memory (mapped): " << allocated << " (" << mapped
                                << ")        Dirty page: " << get_jemalloc_dirty_page_count();
        /* enable back ground thread to recycle memory hold by idle threads. It impacts performance. Enable it only if
        dirty page count is too high. bool set_background_thread = true; size_t sz_background_thread =
        sizeof(set_background_thread); if (mallctl("background_thread", NULL, NULL, &set_background_thread,
        sz_background_thread) != 0) { LOG(WARNING) << "fail to enable back ground thread for jemalloc";
        } */
    }
    if (mallctl("stats.allocated", &allocated, &sz_allocated, NULL, 0) != 0) {
        allocated = 0;
    }
#endif
    return allocated;
}

#include <ifaddrs.h>
#include <arpa/inet.h>

bool is_local_addr(struct sockaddr* addr) {
    bool ret = false;
    struct ifaddrs* interfaces = nullptr;
    struct ifaddrs* temp_addr = nullptr;

    auto error = getifaddrs(&interfaces);
    if (error != 0) {
        return false;
    }

    constexpr int max_size = 4 * sizeof("123");
    static char client_ip[max_size];
    snprintf(client_ip, sizeof(client_ip), "%s", inet_ntoa(((struct sockaddr_in*)addr)->sin_addr));
    LOG(INFO) << "Checking if client ip is local: " << client_ip;

    temp_addr = interfaces;
    while (temp_addr != nullptr) {
        if (temp_addr->ifa_addr->sa_family == AF_INET) {
            char* server_ip = inet_ntoa(((struct sockaddr_in*)temp_addr->ifa_addr)->sin_addr);
            if (strncmp(server_ip, client_ip, max_size) == 0) {
                ret = true;
                break;
            }
        }
        temp_addr = temp_addr->ifa_next;
    }

    freeifaddrs(interfaces);
    return ret;
}

struct recursive_directory_range {
    typedef boost::filesystem::recursive_directory_iterator iterator;
    recursive_directory_range(boost::filesystem::path p) : p_(p) {}

    iterator begin() { return boost::filesystem::recursive_directory_iterator(p_); }
    iterator end() { return boost::filesystem::recursive_directory_iterator(); }

    boost::filesystem::path p_;
};

void keep_recent_n_files(const std::string& dir_path, const std::string& regex, uint32_t max_files_to_keep) {
    std::vector<std::string> all_matching_files;
    boost::regex filter(regex);
    CVLOG(VMODULE_ADMIN, 4) << "keep_recent_n_files: " << dir_path << ", regex: " << regex
                            << ", max files: " << max_files_to_keep;
    try {
        for (auto i : recursive_directory_range(dir_path)) {
            // Skip if not a file
            if (!boost::filesystem::is_regular_file(i.status()))
                continue;

            boost::smatch what;
            CVLOG(VMODULE_ADMIN, 4) << "file: " << i.path().generic_string() << ", regex: " << filter
                                    << ",matches: " << boost::regex_match(i.path().filename().string(), what, filter);
            if (!boost::regex_match(i.path().filename().string(), what, filter))
                continue;

            // File matches, store it
            all_matching_files.push_back(i.path().generic_string());
        }
    } catch (boost::filesystem::filesystem_error& fex) {
        LOG(ERROR) << fex.what();
        return;
    }

    if (all_matching_files.size() < max_files_to_keep) {
        CVLOG(VMODULE_ADMIN, 4) << "Matched files are less than max files to keep: " << all_matching_files.size()
                                << " < " << max_files_to_keep;
        return;
    }

    // last_write_time retrieval can introduce exception
    try {
        std::sort(all_matching_files.begin(), all_matching_files.end(), [](const std::string& a, const std::string& b) {
            return boost::filesystem::last_write_time(a) > boost::filesystem::last_write_time(b);
        });
    } catch (boost::filesystem::filesystem_error& fex) {
        LOG(ERROR) << fex.what();
        return;
    }

    // Keep only the most recent files
    for (uint32_t idx = max_files_to_keep; idx < all_matching_files.size(); idx++) {
        try {
            boost::filesystem::remove(all_matching_files[idx]);
        } catch (boost::filesystem::filesystem_error& fex) {
            LOG(ERROR) << fex.what();
        }
    }
}

template <typename T, typename create_fn, auto destroy_fn, typename... Args>
custom_unique_ptr<T, destroy_fn> make_custom_unique(Args&&... args) {
    custom_unique_ptr<T, destroy_fn> ptr{create_fn(std::forward<Args>(args)...)};
    return ptr;
}

#if defined(_WIN32) || defined(_WIN64) || defined(WINDOWS)
#include <direct.h>
#define _get_cwd _getcwd
#else
#include <unistd.h>
#define _get_cwd getcwd
#endif

std::string get_cwd() {
    char buff[FILENAME_MAX];
    if (_get_cwd(buff, FILENAME_MAX) != nullptr)
        return std::string(buff);
    else
        return std::string{};
}

std::string get_cwd_path(const std::string& path) {
    auto cwd = get_cwd();
    if (cwd[cwd.size() - 1] == '/') {
        return cwd.append(path);
    } else {
        return cwd.append("/").append(path);
    }
}

std::string getNowTimeStr() {
    time_t rawtime;
    struct tm* timeinfo;
    char buffer[80];
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    strftime(buffer, sizeof(buffer), "%d-%m-%Y %H:%M:%S", timeinfo);
    return std::string(buffer);
}
