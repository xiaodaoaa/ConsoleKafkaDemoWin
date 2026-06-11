#pragma once

#ifndef KAFKA_CLIENT_PRODUCER_H
#define KAFKA_CLIENT_PRODUCER_H

#include "Types.h"
#include "Config.h"

#include <kafka/rdkafkacpp.h>

namespace KafkaClient {

class Producer : public RdKafka::DeliveryReportCb, public RdKafka::EventCb {
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
    void dr_cb(RdKafka::Message& message) override;
    void event_cb(RdKafka::Event& event) override;

    ProducerConfig m_config;
    RdKafka::Producer* m_producer = nullptr;
    RdKafka::Topic* m_topic = nullptr;
    DeliveryCallback m_deliveryCb;
    bool m_initialized = false;
};

} // namespace KafkaClient

#endif // KAFKA_CLIENT_PRODUCER_H
