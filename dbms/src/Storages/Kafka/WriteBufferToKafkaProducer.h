#pragma once

#include <IO/WriteBuffer.h>

#include <cppkafka/cppkafka.h>

#include <list>

namespace DB
{

class WriteBufferToKafkaProducer : public WriteBuffer
{
public:
    WriteBufferToKafkaProducer(
        cppkafka::Configuration & conf,
        const std::string & topic_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_,
        std::chrono::milliseconds poll_timeout);
    ~WriteBufferToKafkaProducer() override;

    void count_row();
    void flush();

private:
    void nextImpl() override;

    std::unique_ptr<cppkafka::Producer> producer;
    const std::string topic;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;
    const std::chrono::milliseconds timeout;

    size_t rows = 0;
    std::list<std::string> chunks;
};

}
