# Kafka Client 库重构 - 收尾实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成 Kafka Client 库重构的剩余工作：创建 demo 示例、配置文件、修复 Config.cpp 为 JSON 解析、修复 Producer 回调、修复 Consumer Close bug

**Architecture:** 5 个独立任务，按依赖顺序执行：先修复底层库问题，再创建上层示例

**Tech Stack:** C++11, librdkafka, 简易 JSON 解析器（无外部依赖）

---

## 文件变更总览

| 操作 | 文件 | 说明 |
|------|------|------|
| 修改 | `src/Config.cpp` | JSON 解析实现 |
| 修改 | `src/Producer.cpp` | 修复 delivery callback 触发 |
| 修改 | `src/Consumer.cpp` | 修复 Close() 多分区 bug |
| 创建 | `demo/main.cpp` | 示例程序 |
| 创建 | `config/producer.json` | Producer 配置示例 |
| 创建 | `config/consumer.json` | Consumer 配置示例 |

---

### Task 1: Config.cpp 改为 JSON 解析

**Files:**
- Modify: `src/Config.cpp`

JSON 格式示例：
```json
{
  "brokers": "localhost:9092",
  "topic": "test-topic",
  "partition": 0
}
```

- [ ] **Step 1: 编写简易 JSON 解析器**

实现 `static std::string parseJsonString(const std::string& json, const std::string& key)` 和 `static int parseJsonInt(const std::string& json, const std::string& key, int defaultVal)`

```cpp
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

static std::string Trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n\"");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n\",");
    return s.substr(start, end - start + 1);
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
```

- [ ] **Step 2: 验证编译通过**

```bash
cd "D:\Workspace\AOldSource\ConsoleKafkaDemoAI\src"
cl /c /I"../include" /EHsc Config.cpp
```

---

### Task 2: Producer.cpp 修复 DeliveryCallback

**Files:**
- Modify: `src/Producer.cpp`

当前问题：`SetDeliveryCallback()` 设置了回调但 `Send()` 从未触发。设计文档要求"同步发送，等待投递确认"。

- [ ] **Step 1: 修改 Send() 实现同步等待并触发回调**

```cpp
ErrorCode Producer::Send(const std::string& payload, const std::string& key) {
    if (!m_initialized) {
        return ErrorCode::NOT_INITIALIZED;
    }

    RdKafka::ErrorCode resp = m_producer->produce(
        m_topic, m_config.partition,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(payload.c_str()), payload.size(),
        key.empty() ? nullptr : &key, nullptr);

    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Produce failed: " << RdKafka::err2str(resp) << std::endl;
        if (m_deliveryCb) {
            Message msg;
            msg.payload = payload;
            msg.key = key;
            m_deliveryCb(ErrorCode::PRODUCE_FAIL, msg);
        }
        return ErrorCode::PRODUCE_FAIL;
    }

    // 同步等待投递确认
    int waitCount = 0;
    const int maxWait = 100; // 最多等待 100 次 poll
    while (waitCount < maxWait) {
        m_producer->poll(10);
        
        // 检查 outqueue 是否为空（消息已送出）
        if (m_producer->outq_len() == 0) {
            break;
        }
        waitCount++;
    }

    // 触发回调（即使超时也通知）
    if (m_deliveryCb) {
        Message msg;
        msg.payload = payload;
        msg.key = key;
        ErrorCode err = (waitCount >= maxWait) ? ErrorCode::PRODUCE_FAIL : ErrorCode::OK;
        m_deliveryCb(err, msg);
    }

    return (waitCount >= maxWait) ? ErrorCode::PRODUCE_FAIL : ErrorCode::OK;
}
```

- [ ] **Step 2: 验证编译通过**

```bash
cl /c /I"../include" /EHsc Producer.cpp
```

---

### Task 3: Consumer.cpp 修复 Close() 多分区 bug

**Files:**
- Modify: `src/Consumer.cpp`

当前问题：`Close()` 中 `m_consumer->stop(m_topic, m_config.partition)` 当 `partition == -1` 时只停止一个分区，其他分区未清理。

- [ ] **Step 1: 修改 Close() 正确处理多分区**

```cpp
void Consumer::Close() {
    // 停止所有分区的消费
    if (m_consumer && m_topic) {
        if (m_config.partition == -1) {
            for (int32_t i = 0; i < m_config.numPartitions; i++) {
                m_consumer->stop(m_topic, i);
            }
        } else {
            m_consumer->stop(m_topic, m_config.partition);
        }
        m_consumer->poll(1000);
    }
    if (m_topic) {
        delete m_topic;
        m_topic = nullptr;
    }
    if (m_consumer) {
        delete m_consumer;
        m_consumer = nullptr;
    }
    m_initialized = false;
    m_running = false;
}
```

- [ ] **Step 2: 验证编译通过**

```bash
cl /c /I"../include" /EHsc Consumer.cpp
```

---

### Task 4: 创建 config 配置文件示例

**Files:**
- Create: `config/producer.json`
- Create: `config/consumer.json`

- [ ] **Step 1: 创建 producer.json**

```json
{
  "brokers": "localhost:9092",
  "topic": "test-topic",
  "partition": 0,
  "queue.buffering.max.ms": 5
}
```

- [ ] **Step 2: 创建 consumer.json**

```json
{
  "brokers": "localhost:9092",
  "topic": "test-topic",
  "group-id": "test-group",
  "partition": 0,
  "num.partitions": 3,
  "initial.offset": -1,
  "consume.timeout.ms": 1000,
  "auto.offset.reset": "earliest"
}
```

---

### Task 5: 创建 demo/main.cpp 示例程序

**Files:**
- Create: `demo/main.cpp`

- [ ] **Step 1: 编写 demo 程序**

```cpp
#include "kafka_client/Consumer.h"
#include "kafka_client/Producer.h"
#include "kafka_client/Config.h"
#include <iostream>
#include <thread>
#include <chrono>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " [consumer|producer] [config.json]" << std::endl;
        return 1;
    }

    std::string mode = argv[1];
    std::string configFile = (argc >= 3) ? argv[2] : 
        (mode == "consumer" ? "config/consumer.json" : "config/producer.json");

    if (mode == "consumer") {
        auto config = KafkaClient::LoadConsumerConfig(configFile);
        if (config.brokers.empty() || config.topic.empty() || config.groupId.empty()) {
            std::cerr << "Invalid consumer config" << std::endl;
            return 1;
        }

        KafkaClient::Consumer consumer(config);
        consumer.SetMessageCallback([](const KafkaClient::Message& msg) {
            std::cout << "[" << msg.partition << ":" << msg.offset << "] "
                      << msg.payload << std::endl;
        });

        if (auto err = consumer.Init(); err != KafkaClient::ErrorCode::OK) {
            std::cerr << "Init failed: " << static_cast<int>(err) << std::endl;
            return 1;
        }

        std::cout << "Consumer started, press Ctrl+C to stop..." << std::endl;
        consumer.Start();
        consumer.Close();
    } 
    else if (mode == "producer") {
        auto config = KafkaClient::LoadProducerConfig(configFile);
        if (config.brokers.empty() || config.topic.empty()) {
            std::cerr << "Invalid producer config" << std::endl;
            return 1;
        }

        KafkaClient::Producer producer(config);
        producer.SetDeliveryCallback([](KafkaClient::ErrorCode err, const KafkaClient::Message& msg) {
            if (err == KafkaClient::ErrorCode::OK) {
                std::cout << "Delivered: " << msg.payload << std::endl;
            } else {
                std::cerr << "Delivery failed: " << static_cast<int>(err) << std::endl;
            }
        });

        if (auto err = producer.Init(); err != KafkaClient::ErrorCode::OK) {
            std::cerr << "Init failed: " << static_cast<int>(err) << std::endl;
            return 1;
        }

        std::cout << "Producer ready. Enter messages (type 'quit' to exit):" << std::endl;
        std::string line;
        while (true) {
            std::getline(std::cin, line);
            if (line == "quit") break;
            if (line.empty()) continue;
            
            auto err = producer.Send(line);
            if (err != KafkaClient::ErrorCode::OK) {
                std::cerr << "Send failed: " << static_cast<int>(err) << std::endl;
            }
        }

        producer.Close();
    } 
    else {
        std::cerr << "Unknown mode: " << mode << std::endl;
        return 1;
    }

    return 0;
}
```

- [ ] **Step 2: 验证编译通过**

```bash
cl /c /I"../include" /EHsc demo/main.cpp
```

---

## 自审清单

- [ ] 所有步骤都有具体代码，无 "TBD" / "TODO" 占位符
- [ ] Config.cpp JSON 解析覆盖所有配置字段
- [ ] Producer Send 同步等待 + 回调触发
- [ ] Consumer Close 正确处理 partition == -1
- [ ] demo/main.cpp 支持 consumer 和 producer 两种模式
- [ ] 配置文件格式与 Config.cpp 解析逻辑一致