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

// default metadata version that the current code uses to commit to Kafka.
int getDefaultMetadataVersion() { return 0; }

/*
 * Latest metadata version that the current code can work with.
 *
 * If getDefaultMetadataVersion returns 0, but getLatestMetadataVersion returns 1, that means:
 * Current code can understand metadata versions 0 and 1, but it commits its metdata with verion 0.
 * We can ask the server to use version 1 to commits its metdata using HTTP API setMetadataVersion?version=1
 */
int getLatestMetdataVersion() { return 1; }
