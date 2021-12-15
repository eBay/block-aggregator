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

#ifndef _NUCOLUMNAR_HASHING_HPP_
#define _NUCOLUMNAR_HASHING_HPP_

#ifdef _MSC_VER
#pragma once
#endif

#include <string>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <functional>

namespace hashing {
typedef uint8_t(md5_digest_t)[16];
typedef uint8_t(murmur3_32_digest_t)[4];
typedef uint8_t(murmur3_128_digest_t)[16];
typedef uint8_t(murmur3_128_digest_t)[16];
typedef uint8_t(sha1_digest_t)[20];
typedef uint8_t(sha256_digest_t)[32];
typedef uint8_t(sha512_digest_t)[64];

size_t hash_to_long(const void* hash, size_t length);

std::string hash_to_hex(const void* hash, size_t length);

void md5(const void* value, size_t length, void* out);

size_t md5(const void* value, size_t length);

std::string md5_binary(const void* value, size_t length);

void sha1(const void* value, size_t length, void* out);

size_t sha1(const void* value, size_t length);

std::string sha1_binary(const void* value, size_t length);

void sha256(const void* value, size_t length, void* out);

size_t sha256(const void* value, size_t length);

std::string sha256_binary(const void* value, size_t length);

void sha512(const void* value, size_t length, void* out);

size_t sha512(const void* value, size_t length);

std::string sha512_binary(const void* value, size_t length);

void murmur3_32(const void* value, size_t length, void* out, const uint32_t seed = 0);

size_t murmur3_32(const void* value, size_t length, const uint32_t seed = 0);

std::string murmur3_32_binary(const void* value, size_t length, const uint32_t seed = 0);

void murmur3_128(const void* value, size_t length, void* out, const uint32_t seed = 0);

size_t murmur3_128(const void* value, size_t length, const uint32_t seed = 0);

std::string murmur3_128_binary(const void* value, size_t length, const uint32_t seed = 0);

//
// Hash Algorithm Factory.
//
//
enum algorithm_t { Identity = 0, Md5 = 1, Sha_1 = 2, Sha_256 = 3, Sha_512 = 4, Murmur3_32 = 5, Murmur3_128 = 6 };
//
// static const char* ALGORITHM_NAMES[] = {"Identity", "Md5", "Sha_1", "Sha_256", "Sha_512", "Murmur3_32",
// "Murmur3_128"};

// using algorithm_t = monstor::HashAlgorithm;

/**
 * value: for Identity, it's uint64_t, for other algorithms, it's byte[length] array.
 */
inline size_t hash_code(algorithm_t algorithm, const void* value, size_t length) {
    switch (algorithm) {
    case algorithm_t::Murmur3_128:
        return murmur3_128(value, length);
    case algorithm_t::Murmur3_32:
        return murmur3_32(value, length);
    case algorithm_t::Sha_512:
        return sha512(value, length);
    case algorithm_t::Sha_256:
        return sha256(value, length);
    case algorithm_t::Sha_1:
        return sha1(value, length);
    case algorithm_t::Md5:
        return md5(value, length);
    case algorithm_t::Identity:
        return *(uint64_t*)value;
    default:
        throw std::invalid_argument("Invalid algorithm enum");
    }
}

inline std::string hash_binary(algorithm_t algorithm, const void* value, size_t length) {
    switch (algorithm) {
    case algorithm_t::Murmur3_128:
        return murmur3_128_binary(value, length);
    case algorithm_t::Murmur3_32:
        return murmur3_32_binary(value, length);
    case algorithm_t::Sha_512:
        return sha512_binary(value, length);
    case algorithm_t::Sha_256:
        return sha256_binary(value, length);
    case algorithm_t::Sha_1:
        return sha1_binary(value, length);
    case algorithm_t::Md5:
        return md5_binary(value, length);
    case algorithm_t::Identity:
        return std::string(*(uint64_t*)value, sizeof(uint64_t) / sizeof(uint8_t));
    default:
        throw std::invalid_argument("Invalid algorithm enum");
    }
}

inline std::string hash_binary_hex(algorithm_t algorithm, const void* value, size_t length) {
    std::string hash_bin = hash_binary(algorithm, value, length);
    return hash_to_hex(hash_bin.c_str(), hash_bin.size());
}

/**
 * Hash function factory by algorithm type.
 */
inline std::function<size_t(const void*, size_t)> hash_function(algorithm_t algorithm) {
    using namespace std::placeholders;
    return [=](auto&& arg1, auto&& arg2) { return hash_code(algorithm, arg1, arg2); };
}

inline std::function<std::string(const void*, size_t)> hash_binary_function(algorithm_t algorithm) {
    using namespace std::placeholders;
    return [=](auto&& arg1, auto&& arg2) { return hash_binary(algorithm, arg1, arg2); };
}

} // namespace hashing
#endif // _NUCOLUMNAR_HASHING_HPP_
