#pragma once

#include <Interpreters/CustomVariablesValuesDiskStorage.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Binary serialization shared by disk- and ZK-backed value storages.
/// Format is versioned; readers refuse unknown versions.
void writeCustomVariableValueSnapshot(const CustomVariableValueSnapshot & snapshot, WriteBuffer & out);

/// Returns std::nullopt if the blob has an unsupported version.
std::optional<CustomVariableValueSnapshot> readCustomVariableValueSnapshot(ReadBuffer & in);

}
