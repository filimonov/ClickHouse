#include <Common/Exception.h>
#include <Common/SensitiveDataMasker.h>

#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
#    pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#    pragma clang diagnostic ignored "-Wundef"
#endif

#include <gtest/gtest.h>


using namespace DB;


TEST(Common, SensitiveDataMasker)
{
    SensitiveDataMasker masker;
    masker.addMaskingRule("all a letters", "a+", "--a--");
    masker.addMaskingRule("all b letters", "b+", "--b--");
    masker.addMaskingRule("all d letters", "d+", "--d--");
    masker.addMaskingRule("all x letters", "x+", "--x--");
    masker.addMaskingRule("rule \"d\" result", "--d--", "*****"); // RE2 regexps are applied one-by-one in order
    std::string x = "aaaaaaaaaaaaa   bbbbbbbbbb cccc aaaaaaaaaaaa d ";
    EXPECT_EQ(masker.wipeSensitiveData(x), 5);
    EXPECT_EQ(x, "--a--   --b-- cccc --a-- ***** ");
#ifndef NDEBUG
    masker.printStats();
#endif
    EXPECT_EQ(masker.wipeSensitiveData(x), 3);
    EXPECT_EQ(x, "----a----   ----b---- cccc ----a---- ***** ");
#ifndef NDEBUG
    masker.printStats();
#endif


    SensitiveDataMasker masker2;
    masker2.addMaskingRule("hide root password", "qwerty123", "******");
    masker2.addMaskingRule("hide SSN", "[0-9]{3}-[0-9]{2}-[0-9]{4}", "000-00-0000");
    masker2.addMaskingRule("hide email", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}", "hidden@hidden.test");

    std::string query = "SELECT id FROM mysql('localhost:3308', 'database', 'table', 'root', 'qwerty123') WHERE ssn='123-45-6789' or "
                        "email='JonhSmith@secret.domain.test'";
    EXPECT_EQ(masker2.wipeSensitiveData(query), 3);
    EXPECT_EQ(
        query,
        "SELECT id FROM mysql('localhost:3308', 'database', 'table', 'root', '******') WHERE "
        "ssn='000-00-0000' or email='hidden@hidden.test'");


#ifndef NDEBUG
    // simple benchmark
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 200000; ++i)
    {
        std::string query2 = "SELECT id FROM mysql('localhost:3308', 'database', 'table', 'root', 'qwerty123') WHERE ssn='123-45-6789' or "
                             "email='JonhSmith@secret.domain.test'";
        masker2.wipeSensitiveData(query2);
    }
    auto finish = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = finish - start;
    std::cout << "Elapsed time: " << elapsed.count() << "s per 200000 calls (" << elapsed.count() * 1000000 / 200000 << "µs per call)"
              << std::endl;
    // I have: "Elapsed time: 3.44022s per 200000 calls (17.2011µs per call)"
    masker2.printStats();
#endif

    SensitiveDataMasker maskerbad;

    // gtest has not good way to check exception content, so just do it manually (see https://github.com/google/googletest/issues/952 )
    try
    {
        maskerbad.addMaskingRule("bad regexp", "**", "");
        ADD_FAILURE() << "addMaskingRule() should throw an error" << std::endl;
    }
    catch (DB::Exception & e)
    {
        EXPECT_EQ(
            std::string(e.what()),
            "SensitiveDataMasker: cannot compile re2: **, error: no argument for repetition operator: *. Look at "
            "https://github.com/google/re2/wiki/Syntax for reference.");
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
    }
    /* catch (...) { // not needed, gtest will react unhandled exception
        FAIL() << "ERROR: Unexpected exception thrown: " << std::current_exception << std::endl; // std::current_exception is part of C++11x
    } */

    EXPECT_EQ(maskerbad.rulesCount(), 0);
    EXPECT_EQ(maskerbad.wipeSensitiveData(x), 0);
}
