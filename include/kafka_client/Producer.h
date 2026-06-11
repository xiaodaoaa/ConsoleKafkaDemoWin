#pragma once

#ifndef KAFKA_CLIENT_PRODUCER_H
#define KAFKA_CLIENT_PRODUCER_H

#include "Types.h"
#include "Config.h"

namespace RdKafka {
class Producer;
class Topic;
}

namespace KafkaClient {

class Producer {
public:
    explicit Producer(const ProducerConfig& config);
    ~Producer();

    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;
    Producer(Producer&& other) noexcept;
    Producer& operator=(Producer&& other) noexcept;

    ErrorCode Init();
    ErrorCode Send(const std::string& payload, const std::string& key = "");
    void SetDeliveryCallback(DeliveryCallback cb);
    void Close();

private:
    ProducerConfig m_config;
    RdKafka::Producer* m_producer = nullptr;
    RdKafka::Topic* m_topic = nullptr;
    DeliveryCallback m_deliveryCb;
    bool m_initialized = false;
};

} // namespace KafkaClient

#endif // KAFKA_CLIENT_PRODUCER_H
