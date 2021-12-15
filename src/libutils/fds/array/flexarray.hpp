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

#ifndef SRC_LIBUTILS_FDS_ARRAY_FLEXARRAY_HPP_
#define SRC_LIBUTILS_FDS_ARRAY_FLEXARRAY_HPP_

#include <memory>
#include <array>
#include <vector>

namespace fds {
template <typename T, int32_t StaticCount> class FlexArray {
  public:
    FlexArray() : m_count(0) {}

    FlexArray(int32_t size) : FlexArray() { m_vec.reserve(size - StaticCount); }

    ~FlexArray() {
        auto c = m_count < StaticCount ? m_count : StaticCount;
        for (auto i = 0u; i < c; i++) {
            T* mem = (T*)&m_arr_mem[i * sizeof(T)];
            mem->~T();
        }
    }
    uint32_t push_back(T& value) {
        if (m_count < StaticCount) {
            get_in_array(m_count) = value;
        } else {
            m_vec.push_back(value);
        }
        m_count++;
        return m_count - 1;
    }

    template <class... Args> uint32_t emplace_back(Args&&... args) {
        if (m_count < StaticCount) {
            void* mem = (void*)&m_arr_mem[m_count * sizeof(T)];
            new (mem) T(std::forward<Args>(args)...);
        } else {
            m_vec.emplace_back(std::forward<Args>(args)...);
        }
        m_count++;
        return m_count - 1;
    }

    const T& operator[](uint32_t n) const {
        if (n < StaticCount) {
            return get_in_array(n);
        } else {
            return m_vec[n - StaticCount];
        }
    }

    T& operator[](uint32_t n) {
        if (n < StaticCount) {
            return get_in_array(n);
        } else {
            return m_vec[n - StaticCount];
        }
    }

    void reset() {
        m_count = 0;
        m_vec.clear();
    }

    size_t size() const { return m_count; }

  private:
    T& get_in_array(uint32_t ind) const {
        T* arr = (T*)m_arr_mem;
        return arr[ind];
    }

  private:
    // Array or Vector of ops this txn is going to handle
    uint8_t m_arr_mem[sizeof(T) * StaticCount];
    std::vector<T> m_vec;
    uint32_t m_count;
};

template <typename T, int32_t StaticCount> class FlexArray<std::shared_ptr<T>, StaticCount> {
  public:
    FlexArray() : m_count(0) {}

    uint32_t push_back(std::shared_ptr<T>& value) {
        if (m_count < StaticCount) {
            m_arr[m_count] = value;
        } else {
            m_vec.push_back(value);
        }
        m_count++;
        return m_count - 1;
    }

    template <class... Args> uint32_t emplace_back(Args&&... args) {
        if (m_count < StaticCount) {
            m_arr[m_count] = std::make_shared<T>(std::forward<Args>(args)...);
        } else {
            m_vec.emplace_back(std::make_shared<T>(std::forward<Args>(args)...));
        }
        m_count++;
        return m_count - 1;
    }

    std::shared_ptr<T> operator[](uint32_t n) {
        if (n < StaticCount) {
            return m_arr[n];
        } else {
            return m_vec[n - StaticCount];
        }
    }

    const std::shared_ptr<T> operator[](uint32_t n) const {
        if (n < StaticCount) {
            return m_arr[n];
        } else {
            return m_vec[n - StaticCount];
        }
    }

    void freeup(uint32_t n) {
        if (n < StaticCount) {
            m_arr[n].reset();
        } else {
            m_vec[n - StaticCount].reset();
        }
    }

    void reset() {
        m_count = 0;
        m_vec.clear();
    }

    size_t size() const { return m_count; }

  private:
    std::array<std::shared_ptr<T>, StaticCount> m_arr;
    std::vector<std::shared_ptr<T>> m_vec;
    uint32_t m_count;
};

template <typename T, int32_t StaticCount> class FlexArray<std::unique_ptr<T>, StaticCount> {
  public:
    FlexArray() : m_count(0) {}

    uint32_t push_back(std::unique_ptr<T>&& value) {
        if (m_count < StaticCount) {
            m_arr[m_count] = std::move(value);
        } else {
            m_vec.push_back(std::move(value));
        }
        m_count++;
        return m_count - 1;
    }

    template <class... Args> uint32_t emplace_back(Args&&... args) {
        if (m_count < StaticCount) {
            m_arr[m_count] = std::make_unique<T>(std::forward<Args>(args)...);
        } else {
            m_vec.emplace_back(std::make_unique<T>(std::forward<Args>(args)...));
        }
        m_count++;
        return m_count - 1;
    }

    std::unique_ptr<T> release(uint32_t n) {
        if (n < StaticCount) {
            auto p = std::move(m_arr[n]);
            return p;
        } else {
            auto p = std::move(m_vec[n - StaticCount]);
            return std::move(p);
        }
    }

    void freeup(uint32_t n) {
        if (n < StaticCount) {
            m_arr[n].reset();
        } else {
            m_vec[n - StaticCount].reset();
        }
    }

    const T* operator[](uint32_t n) const {
        if (n < StaticCount) {
            return m_arr[n].get();
        } else {
            return m_vec[n - StaticCount].get();
        }
    }

    T* operator[](uint32_t n) {
        if (n < StaticCount) {
            return m_arr[n].get();
        } else {
            return m_vec[n - StaticCount].get();
        }
    }

    void reset() {
        m_count = 0;
        m_vec.clear();
    }

    size_t size() const { return m_count; }

  private:
    std::array<std::unique_ptr<T>, StaticCount> m_arr;
    std::vector<std::unique_ptr<T>> m_vec;
    uint32_t m_count;
};
} // namespace fds

#endif /* SRC_LIBUTILS_FDS_ARRAY_FLEXARRAY_HPP_ */
