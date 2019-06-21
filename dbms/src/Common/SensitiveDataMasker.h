

#ifndef NDEBUG
#    include <iostream>
#endif

#include <memory>
#include <set>
#include <string>
#include <vector>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>


//* TODO: option with hyperscan? https://software.intel.com/en-us/articles/why-and-how-to-replace-pcre-with-hyperscan
// re2::set should also work quite fast, but it doesn't return the match position, only which regexp was matched

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;

}


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

        std::atomic<std::uint64_t> matches_count = 0;

        //options
    public:
        MaskingRule(const std::string & name, const std::string & regexpString, const std::string & replacementString)
            : name(name)
            , replacement_string(replacementString)
            , regexp_string(regexpString)
            , regexp(regexp_string, RE2::Quiet)
            , replacement(replacement_string)
        {
            if (!regexp.ok())
                throw DB::Exception(
                    "SensitiveDataMasker: cannot compile re2: " + regexpString + ", error: " + regexp.error()
                        + ". Look at https://github.com/google/re2/wiki/Syntax for reference.",
                    DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
        }
        int apply(std::string & data)
        {
            auto m = RE2::GlobalReplace(&data, regexp, replacement);
            matches_count += m;
            return m;
        }

        const std::string & getName() const { return name; }
        const std::string & getReplacementString() const { return replacement_string; }
        uint64_t getMatchesCount() const { return matches_count; }
    };

    std::vector<std::unique_ptr<MaskingRule>> all_masking_rules;

public:
    SensitiveDataMasker() {}

    SensitiveDataMasker(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);
        Logger * logger = &Logger::get("SensitiveDataMaskerConfigRead");

        std::set<std::string> used_names;

        for (const auto & rule : keys)
        {
            if (startsWith(rule, "rule"))
            {
                auto rule_config_prefix = config_prefix + "." + rule;

                auto rule_name
                    = config.has(rule_config_prefix + ".name") ? config.getString(rule_config_prefix + ".name") : rule_config_prefix;

                if (used_names.count(rule_name) == 0)
                {
                    used_names.insert(rule_name);
                }
                else
                {
                    throw Exception(
                        "query_masking_rules configuration contains more than one rule named '" + rule_name + "'.",
                        ErrorCodes::INVALID_CONFIG_PARAMETER);
                }

                auto regexp = config.has(rule_config_prefix + ".regexp") ? config.getString(rule_config_prefix + ".regexp") : "";

                if (regexp == "")
                {
                    throw Exception(
                        "query_masking_rules configuration, rule '" + rule_name + "' has no <regexp> node or <regexp> is empty.",
                        ErrorCodes::NO_ELEMENTS_IN_CONFIG);
                }

                auto replace = config.getString(rule_config_prefix + ".replace", "******");

                try
                {
                    this->addMaskingRule(rule_name, regexp, replace);
                }
                catch (DB::Exception & e)
                {
                    e.addMessage("while adding query masking rule '" + rule_name + "'.");
                    throw;
                }
            }
            else
            {
                LOG_WARNING(logger, "Unused param " << config_prefix << '.' << rule);
            }
        }
        auto rules_count = this->rulesCount();
        if (rules_count > 0)
        {
            LOG_INFO(logger, rules_count << " query masking rules loaded.");
        }
    }

    void addMaskingRule(const std::string & name, const std::string & regexpString, const std::string & replacementString)
    {
        all_masking_rules.push_back(std::make_unique<MaskingRule>(name, regexpString, replacementString));
    }

    int wipeSensitiveData(std::string & data)
    {
        int matches = 0;
        for (auto & rule : all_masking_rules)
        {
            matches += rule->apply(data);
        }
        return matches;
    }

#ifndef NDEBUG
    // TODO: we can add something like system.query_masking_rules with that data
    void printStats()
    {
        for (auto & rule : all_masking_rules)
        {
            std::cout << rule->getName() << " (replacement to " << rule->getReplacementString() << ") matched " << rule->getMatchesCount()
                      << " times" << std::endl;
        }
    }
#endif

    unsigned long rulesCount() const { return all_masking_rules.size(); }
};


};
