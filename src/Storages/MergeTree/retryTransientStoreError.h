#pragma once

#include <Common/Logger.h>

#include <functional>
#include <string_view>

namespace DB
{

/// Runs `store`, a write of MergeTree metadata (`txn_version.txt`, `mutation_N.txt`), again
/// after a transient storage error, with backoff, for up to 60 seconds. Transient is decided
/// by `isRetryableException`; any other error is rethrown at once, as is the last error when
/// the budget is exhausted or the server is shutting down, so a write that did not land is
/// never hidden. Writes made after the commit point of a transaction, or during its rollback,
/// have no one to report an error to and need this; the same writes on a query path get it too.
void retryTransientStoreError(LoggerPtr log, std::string_view what, const std::function<void()> & store);

}
