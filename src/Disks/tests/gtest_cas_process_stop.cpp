#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasProcessStop.h>
#include <Disks/tests/cas_process_stop_log.h>

#include <csignal>
#include <cstdlib>
#include <thread>

#include <pthread.h>

using DB::Cas::replaceProcessStopHandlersForTest;
using DB::Cas::requestGracefulProcessStop;
using DB::Cas::terminateProcess;

namespace
{

class CasProcessStopEnvironment : public ::testing::Environment
{
public:
    void SetUp() override { DB::Cas::tests::processStopLog().reset(); }
};

/// A test's probe may capture its locals; no later test may reach them through the handlers.
class CasProcessStopResetListener : public ::testing::EmptyTestEventListener
{
public:
    void OnTestEnd(const ::testing::TestInfo &) override { DB::Cas::tests::processStopLog().reset(); }
};

bool appendResetListener()
{
    ::testing::UnitTest::GetInstance()->listeners().Append(new CasProcessStopResetListener);
    return true;
}

}

[[maybe_unused]] const auto * const cas_process_stop_environment
    = ::testing::AddGlobalTestEnvironment(new CasProcessStopEnvironment);
[[maybe_unused]] const bool cas_process_stop_reset_listener = appendResetListener();

TEST(CASProcessStop, ATestFatalHandlerRunsOnce)
{
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    terminateProcess();
    terminateProcess();
    EXPECT_EQ(log.fatal(), 1u);
}

TEST(CASProcessStop, TheGracefulStopIsRequestedOncePerProcess)
{
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    requestGracefulProcessStop();
    requestGracefulProcessStop();
    EXPECT_EQ(log.graceful(), 1u);
    EXPECT_EQ(log.fatal(), 0u);
}

/// The default handlers really end the process: the statement dies by the signal it names, and nothing in
/// the child returns into gtest, which would call `std::exit` with pool threads alive.
TEST(CASProcessStopDeathTest, TheDefaultGracefulStopRaisesSigterm)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT({ replaceProcessStopHandlersForTest({}); requestGracefulProcessStop(); std::_Exit(0); },
                ::testing::KilledBySignal(SIGTERM), "");
}

/// The renewal worker, the remount thread and query threads may all block `SIGTERM`; a stop asked for
/// on such a thread must still reach the process.
TEST(CASProcessStopDeathTest, TheDefaultGracefulStopFromAThreadBlockingSigtermStillStops)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            replaceProcessStopHandlersForTest({});
            std::thread observer([]
            {
                sigset_t blocked;
                sigemptyset(&blocked);
                sigaddset(&blocked, SIGTERM);
                pthread_sigmask(SIG_BLOCK, &blocked, nullptr);
                requestGracefulProcessStop();
            });
            observer.join();
            std::_Exit(0);
        },
        ::testing::KilledBySignal(SIGTERM), "");
}

TEST(CASProcessStopDeathTest, TheDefaultFatalStopAborts)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT({ replaceProcessStopHandlersForTest({}); terminateProcess(); std::_Exit(0); },
                ::testing::KilledBySignal(SIGABRT), "");
}
