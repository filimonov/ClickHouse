#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>

#include <cppkafka/cppkafka.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

class ReadBufferFromKafkaConsumer : public ReadBuffer
{
public:
    ReadBufferFromKafkaConsumer(
        cppkafka::Configuration & conf,
        Poco::Logger * log_,
        size_t max_batch_size,
        size_t poll_timeout_,
        bool intermediate_commit_,
        const std::atomic<bool> & stopped_);
    ~ReadBufferFromKafkaConsumer() override;

    void allowNext() { allowed = true; } // Allow to read next message.
    void commit(); // Commit all processed messages.
    void subscribe(const Names & topics); // Subscribe internal consumer to topics.
    void unsubscribe(); // Unsubscribe internal consumer in case of failure.

    auto pollTimeout() const { return poll_timeout; }

    bool hasMorePolledMessages() const;
    auto rebalanceHappened() const { return rebalance_happened; }
    void storeLastReadMessage() { consumer->store_offset(*current); }

    // Return values for the message that's being read.
    String currentTopic() const { return current->get_topic(); }
    String currentKey() const { return current->get_key(); }
    auto currentOffset() const { return current->get_offset(); }
    auto currentPartition() const { return current->get_partition(); }
    auto currentTimestamp() const { return current->get_timestamp(); }


private:
    using Messages = std::vector<cppkafka::Message>;
    void printSubscription(const String message, const std::vector<std::string> subscription) const;

    std::unique_ptr<cppkafka::Consumer> consumer;
    Poco::Logger * log;
    const size_t batch_size = 1;
    const size_t poll_timeout = 0;
    bool stalled = false;
    bool intermediate_commit = true;
    bool allowed = true;

    const std::atomic<bool> & stopped;

    Messages messages;
    Messages::const_iterator current;

    bool rebalance_happened = false;
    cppkafka::TopicPartitionList assignment;

    bool nextImpl() override;
};

}
