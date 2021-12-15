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

#include <Aggregator/TableColumnsDescription.h>
#include "common/logging.hpp"
#include <boost/functional/hash.hpp>
#include "common/hashing.hpp"

namespace std {
template <> struct hash<nuclm::TableColumnDescription> {
    std::size_t operator()(nuclm::TableColumnDescription const& cdef) const noexcept {
        std::size_t seed = 0;
        boost::hash_combine(seed, cdef.column_name);
        boost::hash_combine(seed, cdef.column_type);
        return seed;
    }
};

template <> struct hash<nuclm::TableColumnsDescription> {
    std::size_t operator()(nuclm::TableColumnsDescription const& tdef) const noexcept {
        auto columns_description = tdef.getColumnsDescription();
        std::size_t seed = boost::hash_range(columns_description.begin(), columns_description.end());
        return seed;
    }
};
} // namespace std

namespace nuclm {

/**
 * Generate hash for a table schema
 */
class TableSchemaHash {
  public:
    /**
     * It turns out that the hash computation based on Boost is platform dependent. Thus we do not choose this method
     * to compute schema hash.
     */
    static std::size_t hash(const nuclm::TableColumnsDescription& table_definition) {
        return std::hash<nuclm::TableColumnsDescription>{}(table_definition);
    }

    /**
     * Concatenate column name/type to form a murmur128 hash. No default expression is included from the current design.
     * We choose this one method to compute schema hash as the computed hash result is platform-independent.
     */
    static std::size_t hash_concat(const nuclm::TableColumnsDescription& table_definition) {
        std::ostringstream oss;
        auto columns_description = table_definition.getColumnsDescription();
        for (auto& cdef : columns_description) {
            oss << cdef.column_name << cdef.column_type;
        }
        auto hash_func = hashing::hash_function(hashing::algorithm_t::Murmur3_128);
        std::string s = oss.str();

        return hash_func(s.c_str(), s.size());
    }
};

} // namespace nuclm
