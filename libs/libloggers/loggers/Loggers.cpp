#include "Loggers.h"

#include <iostream>
#include <Common/SensitiveDataMasker.h>
#include <Poco/SyslogChannel.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "OwnFormattingChannel.h"
#include "OwnPatternFormatter.h"
#include "OwnSplitChannel.h"
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Net/RemoteSyslogChannel.h>
#include <Poco/Path.h>


// TODO: move to libcommon
static std::string createDirectory(const std::string & file)
{
    auto path = Poco::Path(file).makeParent();
    if (path.toString().empty())
        return "";
    Poco::File(path).createDirectories();
    return path.toString();
};

Poco::AutoPtr<Poco::FileChannel> Loggers::createPocoFileChannel(Poco::Util::AbstractConfiguration & config, const std::string & path)
{
    Poco::AutoPtr<Poco::FileChannel> result = new Poco::FileChannel;
    result->setProperty(Poco::FileChannel::PROP_PATH, Poco::Path(path).absolute().toString());
    result->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
    result->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
    result->setProperty(Poco::FileChannel::PROP_COMPRESS, config.getRawString("logger.compress", "true"));
    result->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
    result->setProperty(Poco::FileChannel::PROP_FLUSH, config.getRawString("logger.flush", "true"));
    result->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config.getRawString("logger.rotateOnOpen", "false"));
    return result;
}

Poco::AutoPtr<Poco::Channel> Loggers::createSyslogChannel(Poco::Util::AbstractConfiguration & config, const std::string & cmd_name)
{
    Poco::AutoPtr<Poco::Channel> channel;
    //const std::string & cmd_name = commandName();
    if (config.has("logger.syslog.address"))
    {
        channel = new Poco::Net::RemoteSyslogChannel();
        // syslog address
        channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_LOGHOST, config.getString("logger.syslog.address"));
        if (config.has("logger.syslog.hostname"))
        {
            channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_HOST, config.getString("logger.syslog.hostname"));
        }
        channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_FORMAT, config.getString("logger.syslog.format", "syslog"));
        channel->setProperty(
            Poco::Net::RemoteSyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_USER"));
    }
    else
    {
        channel = new Poco::SyslogChannel();
        channel->setProperty(Poco::SyslogChannel::PROP_NAME, cmd_name);
        channel->setProperty(Poco::SyslogChannel::PROP_OPTIONS, config.getString("logger.syslog.options", "LOG_CONS|LOG_PID"));
        channel->setProperty(Poco::SyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_DAEMON"));
    }
    return channel;
}

Poco::AutoPtr<DB::OwnFormattingChannel> Loggers::wrapChannelWithFormatter(Poco::AutoPtr<Poco::Channel> original_channel, OwnPatternFormatter::Options options)
{
    Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this, options);
    return new DB::OwnFormattingChannel(pf, original_channel);
}

// do we still need old interface for something else?
void Loggers::buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger /*_root*/, const std::string & cmd_name)
{
    Loggers::buildLoggers(config, logger, {}, cmd_name);
}

void Loggers::buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger /*_root*/, const std::shared_ptr<DB::SensitiveDataMasker> sensitive_data_masker, const std::string & cmd_name)
{
    auto current_logger = config.getString("logger", "");
    if (config_logger == current_logger)
        return;
    config_logger = current_logger;

    bool is_daemon = config.getBool("application.runAsDaemon", false);

    /// Split logs to ordinary log, error log, syslog and console.
    /// Use extended interface of Channel for more comprehensive logging.
    Poco::AutoPtr<DB::OwnSplitChannel> split = new DB::OwnSplitChannel;
    if (sensitive_data_masker)
    {
        split->setMasker(sensitive_data_masker);
    }

    auto log_level = config.getString("logger.level", "trace");
    const auto log_path = config.getString("logger.log", "");
    if (!log_path.empty())
    {
        createDirectory(log_path);
        std::cerr << "Logging " << log_level << " to " << log_path << std::endl;

        // Set up two channel chains.
        log_file = createPocoFileChannel(config,log_path);
        log_file->open();

        split->addChannel(wrapChannelWithFormatter(error_log_file));
    }

    const auto errorlog_path = config.getString("logger.errorlog", "");
    if (!errorlog_path.empty())
    {
        createDirectory(errorlog_path);
        std::cerr << "Logging errors to " << errorlog_path << std::endl;

        error_log_file = createPocoFileChannel(config, errorlog_path);
        auto errorlog = wrapChannelWithFormatter(error_log_file);
        errorlog->setLevel(Poco::Message::PRIO_NOTICE);
        errorlog->open();

        split->addChannel(errorlog);
    }

    /// "dynamic_layer_selection" is needed only for Yandex.Metrika, that share part of ClickHouse code.
    /// We don't need this configuration parameter.
    if (config.getBool("logger.use_syslog", false) || config.getBool("dynamic_layer_selection", false))
    {
        syslog_channel = createSyslogChannel(config,cmd_name);
        syslog_channel->open();
        split->addChannel(wrapChannelWithFormatter(syslog_channel, OwnPatternFormatter::ADD_LAYER_TAG));
    }

    if (config.getBool("logger.console", false)
        || (!config.hasProperty("logger.console") && !is_daemon && (isatty(STDIN_FILENO) || isatty(STDERR_FILENO))))
    {
        logger.warning("Logging " + log_level + " to console");
        split->addChannel(wrapChannelWithFormatter(new Poco::ConsoleChannel));
    }

    split->open();
    logger.close();
    logger.setChannel(split);

    // Global logging level (it can be overridden for specific loggers).
    logger.setLevel(log_level);

    // Set level to all already created loggers
    std::vector<std::string> names;
    //logger_root = Logger::root();
    logger.root().names(names);
    for (const auto & name : names)
        logger.root().get(name).setLevel(log_level);

    // Attach to the root logger.
    logger.root().setLevel(log_level);
    logger.root().setChannel(logger.getChannel());

    // Explicitly specified log levels for specific loggers.
    Poco::Util::AbstractConfiguration::Keys levels;
    config.keys("logger.levels", levels);

    if (!levels.empty())
        for (const auto & level : levels)
            logger.root().get(level).setLevel(config.getString("logger.levels." + level, "trace"));
}

void Loggers::closeLogs(Poco::Logger & logger)
{
    if (log_file)
        log_file->close();
    if (error_log_file)
        error_log_file->close();
    // Shouldn't syslog_channel be closed here too?

    if (!log_file)
        logger.warning("Logging to console but received signal to close log file (ignoring).");
}
