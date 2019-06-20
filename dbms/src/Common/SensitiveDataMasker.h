
#include <string>
#include <vector>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <memory>
#include <iostream>


//* TODO: option with hyperscan? https://software.intel.com/en-us/articles/why-and-how-to-replace-pcre-with-hyperscan
// re2::set should also work quite fast, but it doesn't return the match position, only which regexp was matched

namespace DB {

    class SensitiveDataMasker
    {

    private:

        class MaskingRule
        {
        private:
            const std::string name;
            const std::string replacement_string;
            const std::string regexp_string;

            const RE2 regexp;
            const re2::StringPiece replacement;

            uint64_t matches_count = 0;
            std::mutex mutex;

            //options
        public:
            MaskingRule(const std::string &name, const std::string &regexpString,
                        const std::string &replacementString) : name(name),
                                                                replacement_string(replacementString),
                                                                regexp_string(regexpString),
                                                                regexp(regexp_string, RE2::Quiet),
                                                                replacement(replacement_string) {}
            int apply(std::string &data)
            {
                auto m = RE2::GlobalReplace(&data, regexp, replacement);
                std::lock_guard lock(mutex);
                matches_count += m;
                return m;
            }

            const std::string &getName() const {  return name;    }
            const std::string &getReplacementString() const {  return replacement_string;  }
            uint64_t getMatchesCount() const   {  return matches_count;  }
        };

        std::vector<std::unique_ptr<MaskingRule>> all_masking_rules;

    public:

        SensitiveDataMasker() {}

        void addMaskingRule(const std::string &name, const std::string &regexpString, const std::string &replacementString)
        {
            all_masking_rules.push_back(std::make_unique<MaskingRule>(name, regexpString, replacementString));
        }

        int wipeSensitiveData(std::string &data)
        {
            int matches = 0;
            for (auto &rule : all_masking_rules)
            {
                matches += rule->apply(data);
            }
            return matches;
        }

        void printStats()
        {
            for (auto &rule : all_masking_rules)
            {
                std::cout << rule->getName() << " (replacement to " << rule->getReplacementString() << ") matched " << rule->getMatchesCount() << " times" << std::endl;
            }
        }
    };

};
