#pragma once

#ifndef KAFKA_CLIENT_CONSUMER_H
#define KAFKA_CLIENT_CONSUMER_H

#include "Types.h"
#include "Config.h"
#include <mutex>

#include <kafka/rdkafkacpp.h>

namespace KafkaClient {

class Consumer : public RdKafka::EventCb {
public:
    explicit Consumer(const ConsumerConfig& config);
    ~Consumer();

    Consumer(const Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;
    Consumer(Consumer&& other) noexcept;
    Consumer& operator=(Consumer&& other) noexcept;

    ErrorCode Init();
    ErrorCode Start();
    void SetMessageCallback(MessageCallback cb);
    void Stop();
    void Close();

private:
    void event_cb(RdKafka::Event& event) override;
    void ConsumeLoop();
    void ProcessMessage(RdKafka::Message* msg);

    ConsumerConfig m_config;
    RdKafka::Consumer* m_consumer = nullptr;
    RdKafka::Topic* m_topic = nullptr;
    MessageCallback m_msgCb;
    bool m_initialized = false;
    bool m_running = false;
    bool m_partitionsStopped = false;
    std::mutex m_mutex;
};

} // namespace KafkaClient

#endif // KAFKA_CLIENT_CONSUMER_H
