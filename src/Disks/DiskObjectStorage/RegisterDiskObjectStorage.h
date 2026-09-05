#pragma once

#include <base/types.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

/// Whether the `object_storage` disk configured at `config_prefix` should get the CAS client profile's
/// S3 keep-alive defaults: exactly when its `metadata_type` is `cas`. Exposed as a free function so the
/// rule is testable directly, without a full `DiskFactory` round trip.
bool casClientProfileHintFor(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

}
