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

#ifndef MONSTORDATABASE_USEFUL_DEFS_HPP
#define MONSTORDATABASE_USEFUL_DEFS_HPP

namespace fds {
template <class P, class M> inline size_t offset_of(const M P::*member) {
    return (size_t) & (reinterpret_cast<P*>(0)->*member);
}

template <class P, class M> inline P* container_of(const M* ptr, const M P::*member) {
    return (P*)((char*)ptr - offset_of(member));
}
} // namespace fds
#endif // MONSTORDATABASE_USEFUL_DEFS_HPP
