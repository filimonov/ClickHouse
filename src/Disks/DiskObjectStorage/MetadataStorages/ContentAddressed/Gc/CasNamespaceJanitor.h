#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <algorithm>
#include <functional>
#include <vector>

namespace DB::Cas
{

struct NamespaceJanitorResult
{
    uint64_t pages = 0;
    uint64_t keys = 0;
    uint64_t deleted = 0;
    uint64_t leaked = 0;
    std::vector<String> anomalies;
};

/// Runs one bounded, leak-only page over the physical namespace ownership tree.
class NamespaceJanitor
{
public:
    /// `bulk_delete_chunk_keys_` bounds one batch delete of dead-life `_log`/`_snap` keys; it is clamped
    /// to `[1, kBulkDeleteMaxKeys]`.
    NamespaceJanitor(CasRequests & requests_, const Layout & layout_, size_t page_budget_,
                     size_t bulk_delete_chunk_keys_ = kBulkDeleteMaxKeys)
        : requests(requests_), layout(layout_), page_budget(page_budget_)
        , bulk_delete_chunk_keys(std::clamp<size_t>(bulk_delete_chunk_keys_, 1, kBulkDeleteMaxKeys)) {}

    /// `liveness` is admitted once for the whole page (one `CasOperation` covers the read, the list,
    /// every delete and the cursor publication): a fact the fence cannot see, such as "this tenure
    /// still holds the GC round's own lease" -- see `CasRequests::admit`. It is SAMPLED BEFORE EVERY
    /// REQUEST the page makes (and before every reissue of one), not just where this function
    /// itself checks `op.admitted()` -- so it must be cheap and must never throw. A sample
    /// that returns false ends whichever request was about to be sent: the maintenance read and the
    /// list throw out of this call, a refused HEAD is reported as a leak, a refused batch delete falls
    /// back to the per-key path, and a write verb (a delete, the cursor publication) reports `GaveUp`.
    NamespaceJanitorResult runOnePage(bool suppress_deletes, Liveness liveness);

private:
    CasRequests & requests;
    const Layout & layout;
    size_t page_budget;
    size_t bulk_delete_chunk_keys;
};

}
