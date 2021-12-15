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

#include <memory>

template <typename T> class non_null_unique_ptr : public std::unique_ptr<T> {
  public:
    non_null_unique_ptr() noexcept : std::unique_ptr<T>{new T} {};
    non_null_unique_ptr(T* ptr) : std::unique_ptr<T>{ptr} {
        if (!*this) {
            std::unique_ptr<T>::reset(new T);
        }
    }
    non_null_unique_ptr(const non_null_unique_ptr& other) = delete;
    non_null_unique_ptr& operator=(const non_null_unique_ptr<T>& other) = delete;

    ~non_null_unique_ptr() { std::unique_ptr<T>::reset(); }

    non_null_unique_ptr(non_null_unique_ptr&& other) : std::unique_ptr<T>{std::move(other)} { assert(*this); }

    non_null_unique_ptr& operator=(non_null_unique_ptr&& other) {
        assert(other);
        std::unique_ptr<T>::operator=(std::move(other));
        return *this;
    };

    non_null_unique_ptr& operator=(std::unique_ptr<T>&& other) {
        assert(other);
        std::unique_ptr<T>::operator=(std::move(other));
        return *this;
    };

    T& operator*() const {
        assert(*this);
        return std::unique_ptr<T>::operator*();
    }

    T* operator->() const noexcept {
        assert(*this);
        return std::unique_ptr<T>::operator->();
    }

    T* get() const noexcept { return std::unique_ptr<T>::get(); }

    T* release() noexcept {
        assert(*this);
        T* ret = std::unique_ptr<T>::release();
        reset();
        return ret;
    }

    void reset(T* t = nullptr) noexcept {
        if (t == nullptr)
            t = new T;
        std::unique_ptr<T>::reset(t);
    }

    void swap(non_null_unique_ptr& other) noexcept {
        if (!other)
            other.reset(new T);
        std::unique_ptr<T>::swap(other);
    }
};

template <typename T> struct embedded_t : public T {
    embedded_t(T* t) {
        if (t != nullptr) {
            *static_cast<T*>(this) = std::move(*t);
            delete t;
        }
    }
    embedded_t() = default;

    const T* get() const noexcept { return this; }
    T* get() noexcept { return this; }
    const T* operator->() const noexcept { return this; }
    T* operator->() noexcept { return this; }

    const T& operator*() const noexcept { return *this; }
    T& operator*() noexcept { return *this; }

    explicit operator bool() const noexcept { return true; }
};

template <class T> constexpr bool operator==(const embedded_t<T>& x, std::nullptr_t) noexcept { return false; }

template <class T> constexpr bool operator!=(const embedded_t<T>& x, std::nullptr_t) noexcept { return true; }
