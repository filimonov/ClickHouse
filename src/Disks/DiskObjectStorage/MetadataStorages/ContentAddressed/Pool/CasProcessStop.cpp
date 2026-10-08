#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasProcessStop.h>
#include <atomic>
#include <csignal>
#include <exception>
#include <utility>
#include <unistd.h>

namespace DB::Cas
{

namespace
{

ProcessStopHandlers & handlers()
{
    static ProcessStopHandlers installed;
    return installed;
}

std::atomic<bool> & gracefulRequested()
{
    static std::atomic<bool> requested{false};
    return requested;
}

std::atomic<bool> & fatalRequested()
{
    static std::atomic<bool> requested{false};
    return requested;
}

}

void requestGracefulProcessStop() noexcept
{
    if (gracefulRequested().exchange(true))
        return;
    if (const auto & graceful = handlers().graceful)
    {
        try
        {
            graceful();
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
        return;
    }
    /// A stop that cannot be asked for must not leave a split brain running.
    if (::kill(::getpid(), SIGTERM) != 0)
        std::terminate();
}

void terminateProcess() noexcept
{
    if (const auto & fatal = handlers().fatal)
    {
        if (fatalRequested().exchange(true))
            return;
        try
        {
            fatal();
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
        return;
    }
    std::terminate();
}

ProcessStopHandlers replaceProcessStopHandlersForTest(ProcessStopHandlers replacement)
{
    ProcessStopHandlers previous = std::exchange(handlers(), std::move(replacement));
    gracefulRequested().store(false);
    fatalRequested().store(false);
    return previous;
}

}
