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

#include "hashing.hpp"
#include "hash/MurmurHash3.hpp"
#include <openssl/sha.h>
#include <openssl/md5.h>

union longbytes {
    uint8_t b[sizeof(uint64_t)];
    uint64_t l;
};

union intbytes {
    uint8_t b[sizeof(uint32_t)];
    uint32_t i;
};

inline void int_to_bytes(const uint32_t* data, size_t length, void* out) {
    uint8_t* result = reinterpret_cast<uint8_t*>(out);
    int pos = 0;
    for (size_t i = 0; i < length; ++i) {
        intbytes bi;
        bi.i = data[i];
        for (size_t j = 0; j < sizeof(uint32_t); ++j) {
            result[pos++] = bi.b[j];
        }
    }
}

inline void long_to_bytes(const uint64_t* data, size_t length, void* out) {
    uint8_t* result = reinterpret_cast<uint8_t*>(out);
    int pos = 0;
    for (size_t i = 0; i < length; ++i) {
        longbytes bi;
        bi.l = data[i];
        for (size_t j = 0; j < sizeof(uint64_t); ++j) {
            result[pos++] = bi.b[j];
        }
    }
}

static std::string hash_to_binary(const void* bytes, size_t length) {
    return std::string(reinterpret_cast<const char*>(bytes), length);
}

/**
 * Reference Java implementation.
 */
size_t hashing::hash_to_long(const void* bytes, size_t length) {
    const uint8_t* digest = reinterpret_cast<const uint8_t*>(bytes);
    size_t retVal = digest[0] & 0xFF;
    for (size_t i = 1; i < (length > 8 ? 8 : length); i++) {
        retVal |= (digest[i] & 0xFFL) << (i * 8);
    }
    return retVal;
}

std::string hashing::hash_to_hex(const void* bytes, size_t length) {
    const uint8_t* digest = reinterpret_cast<const uint8_t*>(bytes);
    static unsigned char hexmap[] = "0123456789abcdef";
    std::string s(length * 2, ' ');
    for (size_t i = 0; i < length; ++i) {
        s[2 * i] = hexmap[((unsigned char)digest[i] & 0xF0) >> 4];
        s[2 * i + 1] = hexmap[digest[i] & 0x0F];
    }
    return s;
}

void hashing::md5(const void* data, size_t length, void* out) {
    MD5(reinterpret_cast<const uint8_t*>(data), length, reinterpret_cast<uint8_t*>(out));
}

size_t hashing::md5(const void* value, size_t length) {
    md5_digest_t digest;
    md5(value, length, digest);
    return hash_to_long(digest, sizeof(md5_digest_t));
}

std::string hashing::md5_binary(const void* value, size_t length) {
    md5_digest_t digest;
    md5(value, length, digest);
    return hash_to_binary(digest, sizeof(md5_digest_t));
}

void hashing::sha1(const void* data, size_t length, void* out) {
    SHA_CTX sha1;
    SHA1_Init(&sha1);
    SHA1_Update(&sha1, data, length);
    SHA1_Final(reinterpret_cast<uint8_t*>(out), &sha1);
}

size_t hashing::sha1(const void* value, size_t length) {
    sha1_digest_t digest;
    sha1(value, length, digest);
    return hash_to_long(digest, sizeof(sha1_digest_t));
}

std::string hashing::sha1_binary(const void* value, size_t length) {
    sha1_digest_t digest;
    sha1(value, length, digest);
    return hash_to_binary(digest, sizeof(sha1_digest_t));
}

void hashing::sha256(const void* bytes, size_t size, void* out) {
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, bytes, size);
    SHA256_Final(reinterpret_cast<uint8_t*>(out), &sha256);
}

size_t hashing::sha256(const void* value, size_t length) {
    sha256_digest_t digest;
    sha256(value, length, digest);
    return hash_to_long(digest, sizeof(sha256_digest_t));
}

std::string hashing::sha256_binary(const void* value, size_t length) {
    sha256_digest_t digest;
    sha256(value, length, digest);
    return hash_to_binary(digest, sizeof(sha256_digest_t));
}

void hashing::sha512(const void* bytes, size_t size, void* out) {
    SHA512_CTX sha512;
    SHA512_Init(&sha512);
    SHA512_Update(&sha512, bytes, size);
    SHA512_Final(reinterpret_cast<uint8_t*>(out), &sha512);
}

size_t hashing::sha512(const void* value, size_t length) {
    sha512_digest_t digest;
    sha512(value, length, digest);
    return hash_to_long(digest, sizeof(sha512_digest_t));
}

std::string hashing::sha512_binary(const void* value, size_t length) {
    sha512_digest_t digest;
    sha512(value, length, digest);
    return hash_to_binary(digest, sizeof(sha512_digest_t));
}

void hashing::murmur3_32(const void* bytes, size_t size, void* digest, const uint32_t seed) {
    uint32_t result;
    MurmurHash3_x86_32(bytes, size, seed, &result);
    int_to_bytes(&result, 1, digest);
}

size_t hashing::murmur3_32(const void* value, size_t length, const uint32_t seed) {
    murmur3_32_digest_t digest;
    murmur3_32(value, length, digest, seed);
    return hash_to_long(digest, sizeof(murmur3_32_digest_t));
}

std::string hashing::murmur3_32_binary(const void* value, size_t length, const uint32_t seed) {
    murmur3_32_digest_t digest;
    murmur3_32(value, length, digest, seed);
    return hash_to_binary(digest, sizeof(murmur3_32_digest_t));
}

void hashing::murmur3_128(const void* bytes, size_t size, void* digest, const uint32_t seed) {
    uint64_t out[2];
    MurmurHash3_x64_128(bytes, size, seed, &out);
    long_to_bytes(out, 2, digest);
}

size_t hashing::murmur3_128(const void* value, size_t length, const uint32_t seed) {
    murmur3_128_digest_t digest;
    murmur3_128(value, length, digest, seed);
    return hash_to_long(digest, sizeof(murmur3_128_digest_t));
}

std::string hashing::murmur3_128_binary(const void* value, size_t length, const uint32_t seed) {
    murmur3_128_digest_t digest;
    murmur3_128(value, length, digest, seed);
    return hash_to_binary(digest, sizeof(murmur3_128_digest_t));
}
