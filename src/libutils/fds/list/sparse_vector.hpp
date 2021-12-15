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

#include <vector>
#include <cassert>

#ifndef MONSTORDATABASE_SPARSE_VECTOR_HPP
#define MONSTORDATABASE_SPARSE_VECTOR_HPP

namespace fds {
/*
 * This class provides additional functionality to std::vector where the entries can be sparse. So it can be
 * inserted in any order and can be looked up with index, like regular array.
 *
 * One additional expectation to std::vector is that the type it carries should support default no argument constructor
 */
template <typename T> class sparse_vector : public std::vector<T> {
  public:
    T& operator[](const size_t index) {
        fill_void(index);
        return std::vector<T>::operator[](index);
    }

    bool index_exists(const size_t index) const { return (index < std::vector<T>::size()); }

    T& at(const size_t index) {
        fill_void(index);
        return std::vector<T>::at(index);
    }

    const T& operator[](const size_t index) const {
        assert(index < std::vector<T>::size());
        return std::vector<T>::operator[](index);
    }

    const T& at(const size_t index) const { return std::vector<T>::at(index); }

  private:
    void fill_void(const size_t index) {
        for (auto i = std::vector<T>::size(); i <= index; i++) {
            std::vector<T>::emplace_back();
        }
    }
};

} // namespace fds
#endif // MONSTORDATABASE_SPARSE_VECTOR_HPP
