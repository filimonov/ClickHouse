#include <Common/SensitiveDataMasker.h>

#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
    #pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
    #pragma clang diagnostic ignored "-Wundef"
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
    masker.printStats();
    EXPECT_EQ(x, "--a--   --b-- cccc --a-- ***** ");

   // SensitiveDataMasker masker2;
}
