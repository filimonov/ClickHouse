#pragma once
#include <boost/noncopyable.hpp>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

namespace DB
{

/// Creation-time hints threaded through `ObjectStorageFactory::create` to the concrete creator, so the
/// disk-registration layer can influence how the underlying object storage is built without teaching
/// the factory itself about CAS (or any other metadata storage).
struct ObjectStorageCreateHints
{
    /// Apply the CAS client profile's S3 keep-alive defaults (`S3ObjectStorage::casClientProfile`) to
    /// the disk's S3 client. Set by `RegisterDiskObjectStorage` when the disk's `metadata_type` is `cas`.
    bool cas_client_profile = false;
};

class ObjectStorageFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<ObjectStoragePtr(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool skip_access_check,
        const ObjectStorageCreateHints & hints)>;

    static ObjectStorageFactory & instance();

    void registerObjectStorageType(const std::string & type, Creator creator);

    /// Whether `type` (e.g. `"s3"`, `"local"`) already has a registered creator -- lets a caller that
    /// does not own the registration's lifetime (a unit test sharing a process-wide registry with other
    /// test suites) register only when needed, instead of risking `registerObjectStorageType`'s
    /// "not unique" throw against a registration some other, already-run suite left in place.
    bool isRegistered(const std::string & type) const;

    ObjectStoragePtr create(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool skip_access_check,
        const ObjectStorageCreateHints & hints = {}) const;

    void clearRegistry();

private:
    using Registry = std::unordered_map<String, Creator>;
    Registry registry;
};

}
