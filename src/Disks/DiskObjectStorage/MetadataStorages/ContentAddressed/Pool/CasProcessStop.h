#pragma once
#include <functional>

namespace DB::Cas
{

/// How a CAS mount that can no longer be this pool member ends the process; one lost slot stops every disk.
struct ProcessStopHandlers
{
    /// Empty: `SIGTERM` to this process; the signal listener runs the shutdown. Process-directed, not
    /// `raise`: the observing thread may block `SIGTERM`, as every Poco thread and its pool workers do.
    std::function<void()> graceful;
    std::function<void()> fatal;   /// empty: `std::terminate`
};

/// The first call in the process asks for the graceful stop; later calls do nothing.
void requestGracefulProcessStop() noexcept;
/// Ends the process; returns only when a test installed a `fatal` handler that returns, which runs once.
void terminateProcess() noexcept;
/// Installs `handlers`, re-arms the once-per-process stops; only while no CAS pool runs.
ProcessStopHandlers replaceProcessStopHandlersForTest(ProcessStopHandlers handlers);

}
