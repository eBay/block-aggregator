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
#include <chrono>
#include <string>
#include <vector>
#include <cxxabi.h>
#include <atomic>
#include <cstring>
#include <farmhash.h>
#include <memory>

#include <common/urcu_helper.hpp>

#if defined __GNUC__ || defined __llvm__
#define _likely(x) __builtin_expect(!!(x), 1)
#define _unlikely(x) __builtin_expect(!!(x), 0)
#else
#define _likely(x) (x)
#define _unlikely(x) (x)
#endif

template <typename T> char* nameof(T&& t) {
    int validCppName;
    return abi::__cxa_demangle(typeid(t).name(), NULL, 0, &validCppName);
}

namespace detail {
template <class T> struct value_category { static constexpr char const value[] = "prvalue"; };
template <class T> struct value_category<T&> { static constexpr char const value[] = "lvalue"; };
template <class T> struct value_category<T&&> { static constexpr char const value[] = "xvalue"; };
} // namespace detail

#define PRINT_VALUE_CAT(expr) INFO(#expr << " is a " << ::detail::value_category<decltype((expr))>::value << '\n')

typedef std::chrono::high_resolution_clock Clock;

inline int64_t get_elapsed_time_ns(Clock::time_point t) {
    std::chrono::nanoseconds ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t);
    return ns.count();
}

inline int64_t get_elapsed_time_us(Clock::time_point t) { return get_elapsed_time_ns(t) / 1000; }
inline uint64_t get_elapsed_time_ms(Clock::time_point t) { return get_elapsed_time_us(t) / 1000; }

inline int64_t get_elapsed_time_ns(Clock::time_point t1, Clock::time_point t2) {
    std::chrono::nanoseconds ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1);
    return ns.count();
}

inline int64_t get_elapsed_time_us(Clock::time_point t1, Clock::time_point t2) {
    return get_elapsed_time_ns(t1, t2) / 1000;
}

inline int64_t get_steady_time_since_epoch_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

inline int64_t get_time_since_epoch_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline int64_t get_time_since_epoch_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline int64_t get_time_since_epoch_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}
inline int64_t get_elapsed_time_ms(int64_t t) { return get_time_since_epoch_ms() - t; }

template <typename T>
bool atomic_update_max(std::atomic<T>& max_value, T const& value,
                       std::memory_order order = std::memory_order_acq_rel) noexcept {
    T prev_value = max_value.load(order);
    while (prev_value < value && !max_value.compare_exchange_weak(prev_value, value, order))
        ;
    return prev_value < value;
}

template <typename T>
bool atomic_update_min(std::atomic<T>& min_value, T const& value,
                       std::memory_order order = std::memory_order_acq_rel) noexcept {
    T prev_value = min_value.load(order);
    while (prev_value > value && !min_value.compare_exchange_weak(prev_value, value, order))
        ;
    return prev_value > value;
}

template <auto fn> using deleter_from_fn = std::integral_constant<decltype(fn), fn>;

template <typename T, auto fn> using custom_unique_ptr = std::unique_ptr<T, deleter_from_fn<fn>>;

template <typename T, typename create_fn, auto destroy_fn, typename... Args>
custom_unique_ptr<T, destroy_fn> make_custom_unique(Args&&... args);

// encode binary data as ascii hex string (does not start with 0x)
std::string bin2hex(unsigned char* data, int len);

// calculate md5 of the binary data and convert it to hex string using bin2hex
std::string md5_hex(const void* data, size_t len);

std::string load_file(std::ifstream& file);
std::string load_file(const char* file_name);

char* json_escape(const char* str, int len);

size_t get_total_memory(bool refresh);
bool is_local_addr(struct sockaddr* addr);

std::string Backtrace(int skip = 1);
void backtrace_unwind();

template <typename T> struct array_deleter {
    void operator()(T const* p) { delete[] p; }
};

const std::string& load_settings_schema();
const std::string& load_schema_bfbs();

/*
 * Keep only the most recent 'n_files_to_keep' files in the specified directory matching the specified regex
 * All the files including the files in sub-directories are scanned
 *
 * Precision for recency comparison is seconds. If two files are written within a second, either one them can be chosen
 * for deletion
 *
 * This function does synchronous file IO. Hence, limiting the max scan to 100 files, beyond that this function has to
 * be invoked again.
 * */
void keep_recent_n_files(const std::string& dir, const std::string& regex, uint32_t n_files_to_keep);

template <typename T> class RateWindow {
  public:
    static const uint32_t s_window_size = 30;

    T m_buckets[s_window_size];
    uint32_t m_idx;

    RateWindow() : m_idx(0) { memset(m_buckets, 0, sizeof(m_buckets)); }

    void add(T value) { m_buckets[m_idx++ % s_window_size] = value; }

    T compute_rate() {
        if (m_idx < s_window_size)
            return 0;
        auto curr = m_idx - 1;
        return (m_buckets[curr % s_window_size] - m_buckets[(curr + s_window_size + 1) % s_window_size]);
    }
};

class FpsInterval {
  private:
    Clock::time_point start = Clock::now();

  public:
    uint64_t value() const { return get_elapsed_time_ms(start); }
};

/*
 * Not thread-safe implementation of Fps(frames-per-second).
 * Specifically, update() is not thread safe.
 * Intentionally, not using atomics or locks because the use case does not need thread-safe implementation
 * */
class Fps {
  protected:
    uint32_t m_fpscount;
    FpsInterval m_fpsinterval;

  public:
    Fps() : m_fpscount(0) {}

    void update() {
        // increase the counter by one
        m_fpscount++;
        // reset the counter and the interval after one second
        if (m_fpsinterval.value() > 1000) {
            m_fpscount = 0;
            m_fpsinterval = FpsInterval();
        }
    }

    auto get() const {
        if (m_fpsinterval.value() == 0)
            return 0.0;
        return m_fpscount * 1000.0 / m_fpsinterval.value();
    }
};

#define NAMESPACE_OPEN(_name) namespace _name {
#define NAMESPACE_CLOSE(_name) }

/**
 * @brief A string of bytes with equality operator and hasher.
 * It can be used to model monstordb group key or any other binary string.
 * The bytes of the string are immutable and copied by value.
 * There is a convenient toString()
 *
 */
struct ByteString {
    ByteString() noexcept : length{0}, hash{0} {}
    ByteString(const char* data_, size_t length_) :
            data{std::make_unique<uint8_t[]>(length_)},
            length{length_},
            hash{util::Hash32(data_, length_)},
            hex_str{bin2hex((unsigned char*)data_, length_)} {
        std::memcpy(data.get(), data_, length_);
    }
    ByteString(const ByteString& other) : ByteString((const char*)other.data.get(), other.length) {}
    ByteString(ByteString&& other) = default;
    ByteString& operator=(ByteString&& other) = default;

    constexpr bool operator==(const ByteString& other) const {
        return length == other.length && memcmp(data.get(), other.data.get(), length) == 0;
    }

    friend std::size_t hash_value(const ByteString& value) { return value.hash; }

    constexpr const std::string& toString() const { return hex_str; }

    std::unique_ptr<uint8_t[]> data;
    std::size_t length;
    std::size_t hash;
    std::string hex_str; // for debugging
};

// Hash function for ByteString
struct ByteStringHasher {
    std::size_t operator()(const ByteString& key) const { return key.hash; }
};

/*

 Text attributes
 0    All attributes off
 1    Bold on
 4    Underscore (on monochrome display adapter only)
 5    Blink on
 7    Reverse video on
 8    Concealed on
 */

/*
 Background colors
 40    Black
 41    Red
 42    Green
 43    Yellow
 44    Blue
 45    Magenta
 46    Cyan
 47    White
 */

/*Foreground colors
 30    Black
 31    Red
 32    Green
 33    Yellow
 34    Blue
 35    Magenta
 36    Cyan
 37    White
*/
// NOTE: attribute codes can be combines using ';' separator

#define B_BLACK "40"
#define B_RED "41"
#define B_GREEN "42"
#define B_YELLOW "43"
#define B_BLUE "44"
#define B_MAGENTA "45"
#define B_CYAN "46"
#define B_WHITE "47"

#define F_BLACK "30"
#define F_RED "31"
#define F_GREEN "32"
#define F_YELLOW "33"
#define F_BLUE "34"
#define F_MAGENTA "35"
#define F_CYAN "36"
#define F_WHITE "37"

#define BOLD "1"
#define UNDERSCORE "4"
#define BLINK "5"
#define REVERSE "7"
#define CONCEALED "8"

#define C_START "\x1b["
#define C_SEP ";"
#define C_END "m"

#define ANSI_COLOR_RED C_START F_RED C_END
#define ANSI_COLOR_RED_UNDERSCORE C_START F_RED C_SEP UNDERSCORE C_END
#define ANSI_COLOR_RED_BOLD C_START F_RED C_SEP BOLD C_END
#define ANSI_COLOR_RED_BOLD_UNDERSCORE C_START F_RED C_SEP BOLD C_SEP UNDERSCORE C_END

#define ANSI_COLOR_GREEN C_START F_GREEN C_END
#define ANSI_COLOR_GREEN_UNDERSCORE C_START F_GREEN C_SEP UNDERSCORE C_END
#define ANSI_COLOR_GREEN_BOLD C_START F_GREEN C_SEP BOLD C_END
#define ANSI_COLOR_GREEN_BOLD_UNDERSCORE C_START F_GREEN C_SEP BOLD C_SEP UNDERSCORE C_END

#define ANSI_COLOR_YELLOW C_START F_YELLOW C_END
#define ANSI_COLOR_YELLOW_UNDERSCORE C_START F_YELLOW C_SEP UNDERSCORE C_END
#define ANSI_COLOR_YELLOW_BOLD C_START F_YELLOW C_SEP BOLD C_END
#define ANSI_COLOR_YELLOW_BOLD_UNDERSCORE C_START F_YELLOW C_SEP BOLD C_SEP UNDERSCORE C_END

#define ANSI_COLOR_BLUE C_START F_BLUE C_END
#define ANSI_COLOR_BLUE_UNDERSCORE C_START F_BLUE C_SEP UNDERSCORE C_END
#define ANSI_COLOR_BLUE_BOLD C_START F_BLUE C_SEP BOLD C_END
#define ANSI_COLOR_BLUE_BOLD_UNDERSCORE C_START F_BLUE C_SEP BOLD C_SEP UNDERSCORE C_END

#define ANSI_COLOR_MAGENTA C_START F_MAGENTA C_END
#define ANSI_COLOR_MAGENTA_UNDERSCORE C_START F_MAGENTA C_SEP UNDERSCORE C_END
#define ANSI_COLOR_MAGENTA_BOLD C_START F_MAGENTA C_SEP BOLD C_END
#define ANSI_COLOR_MAGENTA_BOLD_UNDERSCORE C_START F_MAGENTA C_SEP BOLD C_SEP UNDERSCORE C_END

#define ANSI_COLOR_CYAN C_START F_CYAN C_END
#define ANSI_COLOR_CYAN_UNDERSCORE C_START F_CYAN C_SEP UNDERSCORE C_END
#define ANSI_COLOR_CYAN_BOLD C_START F_CYAN C_SEP BOLD C_END
#define ANSI_COLOR_CYAN_BOLD_UNDERSCORE C_START F_CYAN C_SEP BOLD C_SEP UNDERSCORE C_END

#define ANSI_COLOR_BLACK C_START F_BLACK C_END
#define ANSI_COLOR_BLACK_UNDERSCORE C_START F_BLACK C_SEP UNDERSCORE C_END
#define ANSI_COLOR_BLACK_BOLD C_START F_BLACK C_SEP BOLD C_END
#define ANSI_COLOR_BLACK_BOLD_UNDERSCORE C_START F_BLACK C_SEP BOLD C_SEP UNDERSCORE C_END

#define ANSI_COLOR_WHITE C_START F_WHITE C_END
#define ANSI_COLOR_WHITE_UNDERSCORE C_START F_WHITE C_SEP UNDERSCORE C_END
#define ANSI_COLOR_WHITE_BOLD C_START F_WHITE C_SEP BOLD C_END
#define ANSI_COLOR_WHITE_BOLD_UNDERSCORE C_START F_WHITE C_SEP BOLD C_SEP UNDERSCORE C_END

#define ANSI_COLOR_RESET C_START "0" C_END

/**
 * Get current working directory.
 *
 * @return
 */
std::string get_cwd();

/**
 * Get path under current working directory.
 *
 * @return full path.
 */
std::string get_cwd_path(const std::string& path);

/**
 * Get current datetime in formatted text.
 * @return
 */
std::string getNowTimeStr();
