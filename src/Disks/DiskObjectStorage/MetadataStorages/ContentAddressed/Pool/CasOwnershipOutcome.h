#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <cstdint>
#include <exception>
#include <variant>

namespace DB::Cas
{

/// An ownership step that reached no verdict; retry it. `error` is what a startup caller throws.
struct OwnershipTransient { std::exception_ptr error; };

/// An ownership object or the ref catalog was read and cannot be trusted. `error` is what a startup
/// caller throws. `Absent` is one read's 404, not yet corroborated by a listing.
struct OwnershipCorrupt
{
    enum class Cause : uint8_t { Undecodable, Absent, Ambiguous };
    Cause cause;
    std::exception_ptr error;
};

using CatalogObservation = std::variant<RefCatalog, OwnershipTransient, OwnershipCorrupt>;

inline RefCatalog catalogOrThrow(CatalogObservation && observation)
{
    if (auto * transient = std::get_if<OwnershipTransient>(&observation))
        std::rethrow_exception(transient->error);
    if (auto * corrupt = std::get_if<OwnershipCorrupt>(&observation))
        std::rethrow_exception(corrupt->error);
    return std::get<RefCatalog>(std::move(observation));
}

}
