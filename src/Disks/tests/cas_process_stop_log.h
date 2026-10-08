#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasProcessStop.h>
#include <Common/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <base/types.h>
#include <functional>
#include <mutex>
#include <vector>

namespace DB::Cas::tests
{

/// What the process-stop handlers and the `CasPool` stop lines did, in order. While installed, no
/// test in the binary can signal or abort the test process.
class ProcessStopLog
{
public:
    /// Clears the log and installs handlers that record `"graceful:" + probe()` / `"fatal:" + probe()`,
    /// so a test sees the pool's state at the instant the stop was asked for.
    void reset(std::function<String()> probe = {})
    {
        {
            std::lock_guard lock(mutex);
            entries.clear();
        }
        const auto record = [this, probe](const char * kind) { push(kind + (probe ? probe() : String{})); };
        replaceProcessStopHandlersForTest(ProcessStopHandlers{
            .graceful = [record] { record("graceful:"); },
            .fatal = [record] { record("fatal:"); },
        });
    }

    void push(String entry)
    {
        std::lock_guard lock(mutex);
        entries.push_back(std::move(entry));
    }

    std::vector<String> events() const
    {
        std::lock_guard lock(mutex);
        return entries;
    }

    size_t graceful() const { return count("graceful:"); }
    size_t fatal() const { return count("fatal:"); }

private:
    size_t count(std::string_view prefix) const
    {
        std::lock_guard lock(mutex);
        size_t n = 0;
        for (const auto & entry : entries)
            n += entry.starts_with(prefix);
        return n;
    }

    mutable std::mutex mutex;
    std::vector<String> entries;
};

inline ProcessStopLog & processStopLog()
{
    static ProcessStopLog log;
    return log;
}

/// For its lifetime, records every `CasPool` message that starts with `CAS disk '` into
/// `processStopLog()` as `"error-line:" + text` or `"fatal-line:" + text`.
class ScopedStopLineCapture
{
public:
    ScopedStopLineCapture()
        : logger(getLogger("CasPool"))
        , channel(new LineChannel)
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel(Poco::Message::PRIO_ERROR);
    }

    ~ScopedStopLineCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    ScopedStopLineCapture(const ScopedStopLineCapture &) = delete;
    ScopedStopLineCapture & operator=(const ScopedStopLineCapture &) = delete;

private:
    class LineChannel final : public Poco::Channel
    {
    public:
        void log(const Poco::Message & msg) override
        {
            if (!msg.getText().starts_with("CAS disk '"))
                return;
            if (msg.getPriority() == Poco::Message::PRIO_FATAL)
                processStopLog().push("fatal-line:" + msg.getText());
            else if (msg.getPriority() == Poco::Message::PRIO_ERROR)
                processStopLog().push("error-line:" + msg.getText());
        }
    };

    LoggerPtr logger;
    Poco::AutoPtr<LineChannel> channel;
    /// A real reference (shared=true), so the parked previous channel cannot die while ours is installed.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

}
