#include "kafka_client/Config.h"
#include <fstream>
#include <iostream>
#include <sstream>

namespace KafkaClient {

static std::string ReadFile(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        return "";
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

static std::string ParseJsonString(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return "";
    
    pos = json.find(':', pos);
    if (pos == std::string::npos) return "";
    
    pos = json.find('"', pos);
    if (pos == std::string::npos) return "";
    
    size_t end = json.find('"', pos + 1);
    if (end == std::string::npos) return "";
    
    return json.substr(pos + 1, end - pos - 1);
}

static int ParseJsonInt(const std::string& json, const std::string& key, int defaultVal) {
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return defaultVal;
    
    pos = json.find(':', pos);
    if (pos == std::string::npos) return defaultVal;
    
    pos++;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) pos++;
    
    bool negative = false;
    if (pos < json.size() && json[pos] == '-') {
        negative = true;
        pos++;
    }
    
    int result = 0;
    while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
        result = result * 10 + (json[pos] - '0');
        pos++;
    }
    
    return negative ? -result : result;
}

ProducerConfig LoadProducerConfig(const std::string& filePath) {
    ProducerConfig config;
    std::string content = ReadFile(filePath);
    if (content.empty()) {
        std::cerr << "Failed to read config file: " << filePath << std::endl;
        return config;
    }
    
    config.brokers = ParseJsonString(content, "brokers");
    config.topic = ParseJsonString(content, "topic");
    config.partition = ParseJsonInt(content, "partition", 0);
    config.queueBufferingMaxMs = ParseJsonInt(content, "queue.buffering.max.ms", 5);
    
    return config;
}

ConsumerConfig LoadConsumerConfig(const std::string& filePath) {
    ConsumerConfig config;
    std::string content = ReadFile(filePath);
    if (content.empty()) {
        std::cerr << "Failed to read config file: " << filePath << std::endl;
        return config;
    }
    
    config.brokers = ParseJsonString(content, "brokers");
    config.topic = ParseJsonString(content, "topic");
    config.groupId = ParseJsonString(content, "group-id");
    config.partition = ParseJsonInt(content, "partition", 0);
    config.numPartitions = ParseJsonInt(content, "num.partitions", 3);
    config.initialOffset = ParseJsonInt(content, "initial.offset", -1);
    config.consumeTimeoutMs = ParseJsonInt(content, "consume.timeout.ms", 1000);
    config.autoOffsetReset = ParseJsonString(content, "auto.offset.reset");
    if (config.autoOffsetReset.empty()) config.autoOffsetReset = "earliest";
    
    return config;
}

} // namespace KafkaClient