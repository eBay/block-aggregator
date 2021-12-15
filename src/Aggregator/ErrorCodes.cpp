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

namespace nuclm {

namespace ErrorCodes {

extern const int FAILED_TO_CONNECT_TO_SERVER = 9201;
extern const int BAD_ARGUMENTS = 9202;
extern const int NO_DATA_TO_INSERT = 9203;
extern const int UNEXPECTED_PACKET_FROM_SERVER = 9204;
extern const int UNKNOWN_PACKET_FROM_SERVER = 9205;

extern const int CANNOT_RETRIEVE_DEFINED_TABLES = 9206;
extern const int TABLE_DEFINITION_NOT_FOUND = 9207;

extern const int KAFKA_CONNECTOR_CONFIGURATION_ERROR = 9208;
extern const int KAFKA_CONNECTOR_START_ERROR = 9209;

extern const int COLUMN_DEFINITION_NOT_CORRECT = 9210;
extern const int FAILED_TO_PARSE_INSERT_QUERY = 9211;
extern const int FAILED_TO_DESERIALIZE_MESSAGE = 9212;
extern const int FAILED_TO_RECONSTRUCT_BLOCKS = 9213;
extern const int COLUMN_DEFINITION_NOT_MATCHED_WITH_SCHEMA = 9214;

extern const int NO_COLUMN_FOUND_IN_TABLE = 9215;
extern const int BAD_TABLE_DEFINITION_RETRIEVED = 9216;

extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH = 9217;
extern const int ILLEGAL_TYPE_OF_ARGUMENT = 9218;

extern const int LOGICAL_ERROR = 9219;
extern const int UNKNOWN_TYPE = 9220;
extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE = 9221;
extern const int UNEXPECTED_AST_STRUCTURE = 9222;
extern const int DATA_TYPE_CANNOT_HAVE_ARGUMENTS = 9223;
extern const int ARGUMENT_OUT_OF_BOUND = 9224;

extern const int CANNOT_READ_ALL_DATA = 9225;
extern const int TOO_LARGE_STRING_SIZE = 9226;

extern const int CANNOT_ALLOCATE_MEMORY = 9227;

extern const int SERIALIZATION_METHOD_NOT_IMPLEMENTED = 9228;
extern const int CANNOT_READ_ARRAY_FROM_PROTOBUF = 9229;

} // namespace ErrorCodes

} // namespace nuclm
