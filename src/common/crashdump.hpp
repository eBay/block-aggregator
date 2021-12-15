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

#include <string>

/*
 * Macro useful for testing without breakpad
#define DISABLE_BREAKPAD
*/

#if defined(__clang__) && defined(__has_feature)
#define HAVE_SANITIZER __has_feature(address_sanitizer)
#elif defined(__SANITIZE_ADDRESS__)
#define HAVE_SANITIZER __SANITIZE_ADDRESS__
#else
#define HAVE_SANITIZER 0
#endif

// enable breakpad instrumentation only if we are not compiling with sanitizer
#if defined(NDEBUG) && !defined(DISABLE_BREAKPAD) && !HAVE_SANITIZER
#if defined(__APPLE__)
#define BREAKPAD_ON
#define BREAKPAD_ON_APPLE
#else
#define BREAKPAD_ON
#define BREAKPAD_ON_LINUX
#endif //__APPLE__
#endif // endif defined(NDEBUG) && !defined(DISABLE_BREAKPAD) && !HAVE_SANITIZER

std::string create_crash_dir() noexcept;

std::string enable_minidump();

std::string disable_minidump();

std::string generateThreadDump();

std::string getThreadDump();
