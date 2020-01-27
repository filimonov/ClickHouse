#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>

#include <common/logger_useful.h>

#include <cppkafka/cppkafka.h>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

using namespace std::chrono_literals;

ReadBufferFromKafkaConsumer::ReadBufferFromKafkaConsumer(
    cppkafka::Configuration & conf,
    Poco::Logger * log_,
    size_t max_batch_size,
    size_t poll_timeout_,
    bool intermediate_commit_,
    const std::atomic<bool> & stopped_)
    : ReadBuffer(nullptr, 0)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , intermediate_commit(intermediate_commit_)
    , stopped(stopped_)
    , current(messages.begin())
{
    conf.set_offset_commit_callback([this](cppkafka::Consumer&, cppkafka::Error error,
                                          const cppkafka::TopicPartitionList& topic_partitions)
    {
        if (error) {
            LOG_ERROR(log, "Error during commiting: " << error << ' ' << topic_partitions);
        }
        LOG_TRACE(log, "Committed offset: " << topic_partitions);
    });

    conf.set_error_callback([this](cppkafka::KafkaHandleBase& /* handle */, int error, const std::string& reason) {
        LOG_ERROR(log, "Error from librdkafka: " << error << ' ' << reason );
        return true;
    });

    consumer = std::make_unique<cppkafka::Consumer>(conf);
    consumer->set_assignment_callback([this](const cppkafka::TopicPartitionList& topic_partitions) {
        LOG_TRACE(log, "Topics/partitions assigned: " << topic_partitions);
    });
    consumer->set_revocation_callback([this](const cppkafka::TopicPartitionList& topic_partitions) {
        // Rebalance is happening now, and now we have a chance to finish the work
        // with topics/partitions we were working with before rebalance
        LOG_TRACE(log, "Rebalance initiated. Revoking partitions: " << topic_partitions);

        // we can not flush data to target from that point (it is pulled, not pushed)
        // so the best we can now it to
        // 1) repeat last commit in sync mode (async could be still in queue, we need to be sure is is properly committed before rebalance)
        // 2) stop / brake the current reading:
        //     * clean buffered uncommited messages
        //     * set flag / flush

        consumer->commit();

        messages.clear();
        current = messages.begin();
        BufferBase::set(nullptr, 0, 0);

        rebalance_happened = true;
    });

    consumer->set_rebalance_error_callback([this](cppkafka::Error err) {
        LOG_ERROR(log, "Rebalance error: " << err);
    });
    //    consumer->subscribe();
}

ReadBufferFromKafkaConsumer::~ReadBufferFromKafkaConsumer()
{
    /// NOTE: see https://github.com/edenhill/librdkafka/issues/2077
    consumer->unsubscribe();
    consumer->unassign();
    while (consumer->get_consumer_queue().next_event(1s));
}


void ReadBufferFromKafkaConsumer::commit()
{
    LOG_TRACE(log, "Commiting. Polled offset" << consumer->get_offsets_position(consumer->get_assignment()));
    consumer->async_commit();
}

void ReadBufferFromKafkaConsumer::printSubscription(const String message, const std::vector<std::string> subscription) const
{
    LOG_TRACE(log, message << " [" << boost::algorithm::join(subscription, ", ") << " ]");
}

void ReadBufferFromKafkaConsumer::subscribe(const Names & topics)
{
    auto subscription = consumer->get_subscription();

    if (subscription.empty()) {
        LOG_TRACE(log, "Subscription is empty. Subscribing.");
        size_t subscribe_attempts = 0;
        while (true)
        {
            ++subscribe_attempts;
            try
            {
                consumer->subscribe(topics);
            }
            catch (cppkafka::HandleException & e)
            {
                if (subscribe_attempts < 3)
                {
                    LOG_TRACE(log, "Subscription attempt #" << subscribe_attempts << " failed." << e.what() << " Retrying.");
                    continue;
                }
                else
                    throw;
            }
            break;
        }
        printSubscription("Subscribed to topics:", consumer->get_subscription());
    }
    else
    {
        printSubscription("Already subscribed to topics:", subscription);
    }

    stalled = false;

    // While we wait for an assignment after subscribtion, we'll poll zero messages anyway.
    // If we're doing a manual select then it's better to get something after a wait, then immediate nothing.
    // But due to the nature of async pause/resume/subscribe we can't guarantee any persistent state:
    // see https://github.com/edenhill/librdkafka/issues/2455

    LOG_TRACE(log, consumer->get_assignment());

    while (consumer->get_subscription().empty())
    {

            if (nextImpl())
                break;

            // FIXME: if we failed to receive "subscribe" response while polling and destroy consumer now, then we may hang up.
            //        see https://github.com/edenhill/librdkafka/issues/2077
        }
    }
    rebalance_happened = false;
    stalled = false;
}

void ReadBufferFromKafkaConsumer::unsubscribe()
{
    LOG_TRACE(log, "Re-joining claimed consumer after failure");

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);

    consumer->unsubscribe();
}

bool ReadBufferFromKafkaConsumer::hasMorePolledMessages() const {
    return ( current != messages.end() ) && (current + 1 != messages.end());
}

/// Do commit messages implicitly after we processed the previous batch.
bool ReadBufferFromKafkaConsumer::nextImpl()
{
    /// NOTE: ReadBuffer was implemented with an immutable underlying contents in mind.
    ///       If we failed to poll any message once - don't try again.
    ///       Otherwise, the |poll_timeout| expectations get flawn.
    if (stalled || stopped || !allowed || rebalance_happened)
        return false;

    if (current != messages.end()) {
        ++current;
    }

    if (current == messages.end())
    {
        if (intermediate_commit)
            commit();

        /// Don't drop old messages immediately, since we may need them for virtual columns.
        auto new_messages = consumer->poll_batch(batch_size, std::chrono::milliseconds(poll_timeout));
        if (new_messages.empty())
        {
            LOG_TRACE(log, "Stalled");
            stalled = true;
            return false;
        }
        messages = std::move(new_messages);
        current = messages.begin();

        LOG_TRACE(log, "Polled batch of " << messages.size() << " messages");
    }

    if (auto err = current->get_error())
    {
        ++current;

        // TODO: should throw exception instead
        LOG_ERROR(log, "Consumer error: " << err);
        return false;
    }

    // XXX: very fishy place with const casting.
    auto new_position = reinterpret_cast<char *>(const_cast<unsigned char *>(current->get_payload().get_data()));
    BufferBase::set(new_position, current->get_payload().get_size(), 0);
    allowed = false;

    return true;
}

}
