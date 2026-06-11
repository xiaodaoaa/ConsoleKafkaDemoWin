#pragma once

#ifndef KAFKA_CLIENT_TYPES_H
#define KAFKA_CLIENT_TYPES_H

#include <string>
#include <functional>
#include <cstdint>

namespace KafkaClient {

enum class ErrorCode {
    OK = 0,
    INVALID_CONFIG,
    BROKER_CONNECT_FAIL,
    CREATE_PRODUCER_FAIL,
    CREATE_CONSUMER_FAIL,
    CREATE_TOPIC_FAIL,
    PRODUCE_FAIL,
    CONSUME_FAIL,
    NOT_INITIALIZED,
    ALREADY_RUNNING,
};

struct Message {
    std::string payload;
    std::string key;
    int64_t offset = 0;
    int32_t partition = 0;
};

using MessageCallback = std::function<void(const Message& msg)>;
using DeliveryCallback = std::function<void(ErrorCode err, const Message& msg)>;

} // namespace KafkaClient

#endif // KAFKA_CLIENT_TYPES_H
