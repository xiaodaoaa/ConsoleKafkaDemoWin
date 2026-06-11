#pragma once

#ifndef KAFKA_CLIENT_CONFIG_H
#define KAFKA_CLIENT_CONFIG_H

#include "Types.h"
#include <string>

namespace KafkaClient {

struct ProducerConfig {
    std::string brokers;
    std::string topic;
    int32_t partition = 0;
    int queueBufferingMaxMs = 5;
};

struct ConsumerConfig {
    std::string brokers;
    std::string topic;
    std::string groupId;
    int32_t partition = 0;
    int32_t numPartitions = 3;
    int64_t initialOffset = -1;
    int consumeTimeoutMs = 1000;
    std::string autoOffsetReset = "earliest";
};

ProducerConfig LoadProducerConfig(const std::string& filePath);
ConsumerConfig LoadConsumerConfig(const std::string& filePath);

} // namespace KafkaClient

#endif // KAFKA_CLIENT_CONFIG_H
