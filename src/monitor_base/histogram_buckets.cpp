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

#include "histogram_buckets.hpp"
#include <math.h>

// The default histogram buckets used for all metrics across MonstorDB
namespace monitor {
#define X(name, ...) const prometheus::Histogram::BucketBoundaries HistogramBuckets::name = {{__VA_ARGS__}};
HIST_BKTS_TYPES
#undef X
} // namespace monitor
