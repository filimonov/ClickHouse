#include <Interpreters/CustomVariableValueSnapshot.h>

#include <Common/FieldBinaryEncoding.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace
{
constexpr UInt64 format_version = 1;

void writeTimePoint(WriteBuffer & out, std::chrono::system_clock::time_point timepoint)
{
    const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
    writeBinary(static_cast<Int64>(seconds), out);
}

std::chrono::system_clock::time_point readTimePoint(ReadBuffer & in)
{
    Int64 seconds = 0;
    readBinary(seconds, in);
    return std::chrono::system_clock::time_point{std::chrono::seconds(seconds)};
}
}

void writeCustomVariableValueSnapshot(const CustomVariableValueSnapshot & snapshot, WriteBuffer & out)
{
    writeBinary(format_version, out);
    writeStringBinary(snapshot.runtime_type ? snapshot.runtime_type->getName() : String{}, out);
    writeBinary(static_cast<UInt8>(snapshot.has_value), out);
    if (snapshot.has_value)
        encodeField(snapshot.value, out);
    writeBinary(static_cast<UInt8>(snapshot.is_valid), out);
    writeTimePoint(out, snapshot.last_update_time);
    writeTimePoint(out, snapshot.last_successful_update_time);
    writeStringBinary(snapshot.last_update_hostname, out);
    writeStringBinary(snapshot.last_error, out);
    writeStringBinary(snapshot.last_error_type, out);
}

std::optional<CustomVariableValueSnapshot> readCustomVariableValueSnapshot(ReadBuffer & in)
{
    UInt64 version = 0;
    readBinary(version, in);
    if (version != format_version)
        return std::nullopt;

    CustomVariableValueSnapshot snapshot;
    String type_name;
    readStringBinary(type_name, in);
    if (!type_name.empty())
        snapshot.runtime_type = DataTypeFactory::instance().get(type_name);

    UInt8 has_value = 0;
    readBinary(has_value, in);
    snapshot.has_value = (has_value != 0);
    if (snapshot.has_value)
        snapshot.value = decodeField(in);

    UInt8 is_valid = 0;
    readBinary(is_valid, in);
    snapshot.is_valid = (is_valid != 0);
    snapshot.last_update_time = readTimePoint(in);
    snapshot.last_successful_update_time = readTimePoint(in);
    readStringBinary(snapshot.last_update_hostname, in);
    readStringBinary(snapshot.last_error, in);
    readStringBinary(snapshot.last_error_type, in);
    return snapshot;
}

}
