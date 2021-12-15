/************************************************************************
Modifications Copyright 2021, eBay, Inc.

Original Copyright:
See URL: https://github.com/ClickHouse/ClickHouse

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

#include <Common/COW.h>
#include <Core/Types.h>
#include <Columns/IColumn.h>

#include <boost/noncopyable.hpp>
#include <memory>

namespace nuclm {

class ISerializableDataType;

using SerializableDataTypePtr = std::shared_ptr<const ISerializableDataType>;
using SerializableDataTypes = std::vector<SerializableDataTypePtr>;

class ProtobufReader;
class ProtobufWriter;

/** Properties of data type.
 * Contains methods for serialization/deserialization.
 * Implementations of this interface represent a data type (example: UInt8)
 *  or parametric family of data types (example: Array(...)).
 *
 * DataType is totally immutable object. You can always share them.
 */
class ISerializableDataType : private boost::noncopyable {
  public:
    ISerializableDataType();
    virtual ~ISerializableDataType();

    /// Compile time flag. If false, then if C++ types are the same, then SQL types are also the same.
    /// Example: DataTypeString is not parametric: thus all instances of DataTypeString are the same SQL type.
    /// Example: DataTypeFixedString is parametric: different instances of DataTypeFixedString may be different SQL
    /// types. Place it in descendants: static constexpr bool is_parametric = false;

    /// Name of data type (examples: UInt64, Array(String)).
    DB::String getName() const;

    /// Name of data type family (example: FixedString, Array).
    virtual const char* getFamilyName() const = 0;

    /// Data type id. It's used for runtime type checks.
    virtual DB::TypeIndex getTypeId() const = 0;

    /** Serialize to a protobuf. */
    virtual void serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                                   size_t& value_index) const = 0;
    virtual void deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                                     bool& row_added) const = 0;

  protected:
    virtual DB::String doGetName() const;

  public:
    /// Checks that two instances belong to the same type
    virtual bool equals(const ISerializableDataType& rhs) const = 0;

    /// Various properties on behaviour of data type.

    /** The data type is dependent on parameters and types with different parameters are different.
     * Examples: FixedString(N), Tuple(T1, T2), Nullable(T).
     * Otherwise all instances of the same class are the same types.
     */
    virtual bool isParametric() const = 0;

    /** The data type is dependent on parameters and at least one of them is another type.
     * Examples: Tuple(T1, T2), Nullable(T). But FixedString(N) is not.
     */
    virtual bool haveSubtypes() const = 0;

    /** Is it possible to compare for less/greater, to calculate min/max?
     * Not necessarily totally comparable. For example, floats are comparable despite the fact that NaNs compares to
     * nothing. The same for nullable of comparable types: they are comparable (but not totally-comparable).
     */
    virtual bool isComparable() const { return false; }

    /** Numbers, Enums, Date, DateTime. Not nullable.
     */
    virtual bool isValueRepresentedByNumber() const { return false; }

    /** Integers, Enums, Date, DateTime. Not nullable.
     */
    virtual bool isValueRepresentedByInteger() const { return false; }

    /** Unsigned Integers, Date, DateTime. Not nullable.
     */
    virtual bool isValueRepresentedByUnsignedInteger() const { return false; }

    virtual bool isNullable() const { return false; }

    virtual bool lowCardinality() const { return false; }

    /**
     * If this data type can be wrapped in Nullable data type.
     */
    virtual bool canBeInsideNullable() const { return false; }

    /**
     * If the data type can be wrapped in LowCardinality data type
     */
    virtual bool canBeInsideLowCardinality() const { return false; }

  private:
    friend class SerializableDataTypeFactory;
};

/// Some sugar to check data type of IDataType
struct WhichDataType {
    DB::TypeIndex idx;

    WhichDataType(DB::TypeIndex idx_ = DB::TypeIndex::Nothing) : idx(idx_) {}

    WhichDataType(const ISerializableDataType& data_type) : idx(data_type.getTypeId()) {}

    WhichDataType(const ISerializableDataType* data_type) : idx(data_type->getTypeId()) {}

    WhichDataType(const SerializableDataTypePtr& data_type) : idx(data_type->getTypeId()) {}

    bool isUInt8() const { return idx == DB::TypeIndex::UInt8; }
    bool isUInt16() const { return idx == DB::TypeIndex::UInt16; }
    bool isUInt32() const { return idx == DB::TypeIndex::UInt32; }
    bool isUInt64() const { return idx == DB::TypeIndex::UInt64; }
    bool isUInt128() const { return idx == DB::TypeIndex::UInt128; }
    bool isUInt() const { return isUInt8() || isUInt16() || isUInt32() || isUInt64() || isUInt128(); }
    bool isNativeUInt() const { return isUInt8() || isUInt16() || isUInt32() || isUInt64(); }

    bool isInt8() const { return idx == DB::TypeIndex::Int8; }
    bool isInt16() const { return idx == DB::TypeIndex::Int16; }
    bool isInt32() const { return idx == DB::TypeIndex::Int32; }
    bool isInt64() const { return idx == DB::TypeIndex::Int64; }
    bool isInt128() const { return idx == DB::TypeIndex::Int128; }
    bool isInt() const { return isInt8() || isInt16() || isInt32() || isInt64() || isInt128(); }
    bool isNativeInt() const { return isInt8() || isInt16() || isInt32() || isInt64(); }

    bool isFloat32() const { return idx == DB::TypeIndex::Float32; }
    bool isFloat64() const { return idx == DB::TypeIndex::Float64; }
    bool isFloat() const { return isFloat32() || isFloat64(); }

    bool isDate() const { return idx == DB::TypeIndex::Date; }
    bool isDateTime() const { return idx == DB::TypeIndex::DateTime; }
    bool isDateOrDateTime() const { return isDate() || isDateTime(); }

    bool isString() const { return idx == DB::TypeIndex::String; }
    bool isFixedString() const { return idx == DB::TypeIndex::FixedString; }
    bool isStringOrFixedString() const { return isString() || isFixedString(); }

    bool isNullable() const { return idx == DB::TypeIndex::Nullable; }
};

/// IDataType helpers (alternative for IDataType virtual methods with single point of truth)

inline bool isDate(const SerializableDataTypePtr& data_type) { return WhichDataType(data_type).isDate(); }
inline bool isDateOrDateTime(const SerializableDataTypePtr& data_type) {
    return WhichDataType(data_type).isDateOrDateTime();
}

template <typename T> inline bool isUInt8(const T& data_type) { return WhichDataType(data_type).isUInt8(); }

template <typename T> inline bool isUnsignedInteger(const T& data_type) { return WhichDataType(data_type).isUInt(); }

template <typename T> inline bool isInteger(const T& data_type) {
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt();
}

template <typename T> inline bool isFloat(const T& data_type) {
    WhichDataType which(data_type);
    return which.isFloat();
}

template <typename T> inline bool isNativeNumber(const T& data_type) {
    WhichDataType which(data_type);
    return which.isNativeInt() || which.isNativeUInt() || which.isFloat();
}

template <typename T> inline bool isNumber(const T& data_type) {
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt() || which.isFloat();
}

template <typename T> inline bool isColumnedAsNumber(const T& data_type) {
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt() || which.isFloat() || which.isDateOrDateTime();
}

template <typename T> inline bool isString(const T& data_type) { return WhichDataType(data_type).isString(); }

template <typename T> inline bool isFixedString(const T& data_type) { return WhichDataType(data_type).isFixedString(); }

template <typename T> inline bool isStringOrFixedString(const T& data_type) {
    return WhichDataType(data_type).isStringOrFixedString();
}

inline bool isNotDecimalButComparableToDecimal(const SerializableDataTypePtr& data_type) {
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt();
}

inline bool isCompilableType(const SerializableDataTypePtr& data_type) {
    return data_type->isValueRepresentedByNumber();
}

} // namespace nuclm
