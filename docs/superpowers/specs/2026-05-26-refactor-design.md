# Kafka Client 库重构设计

## 背景

当前项目是一个 Kafka producer/consumer 的 Windows 控制台 demo，存在以下问题：

- 内存泄漏：`main()` 中 `new` 的对象未 `delete`
- 硬编码配置：broker 地址、topic、分区号写死在代码中
- `#if 1` 预编译指令切换 producer/consumer 模式
- 全局变量 `g_outfile`
- 中文注释编码损坏
- C 风格代码（NULL、`using namespace std` 在头文件中）
- 无 RAII，手动管理资源
- Consumer 职责不清：消费消息的同时负责写文件

## 目标

将 Kafka producer 和 consumer 抽取为可复用的 C++ 库，提供干净的 API，与 demo 代码分离。

## 设计决策

| 决策项 | 选择 | 理由 |
|--------|------|------|
| 重构方案 | 模块化库结构 | 结构清晰、职责分离、易于测试和扩展 |
| 配置方式 | 构造函数参数 + JSON 配置文件 | 灵活：代码中直接传参或从文件加载 |
| 消息处理 | 回调接口（std::function） | 解耦：库只负责收发，业务逻辑由调用方实现 |
| 错误处理 | 返回值/ErrorCode 枚举 | 简单直接，符合 C++ 传统库风格 |
| 消息持久化 | 库不负责 | 职责单一，写文件等逻辑由调用方在回调中实现 |

## 模块设计

### 1. Types.h — 核心类型定义

所有类型在 `KafkaClient` 命名空间内。

#### ErrorCode 枚举

```cpp
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
```

#### Message 结构体

```cpp
struct Message {
    std::string payload;
    std::string key;
    int64_t offset;
    int32_t partition;
};
```

轻量值类型，不包含 librdkafka 依赖，可安全传递和存储。

#### 回调类型

```cpp
using MessageCallback = std::function<void(const Message& msg)>;
using DeliveryCallback = std::function<void(ErrorCode err, const Message& msg)>;
```

调用方可以用 lambda、函数指针或仿函数。

### 2. Config.h — 配置结构体

#### ProducerConfig

```cpp
struct ProducerConfig {
    std::string brokers;           // 必填
    std::string topic;             // 必填
    int32_t partition = 0;
    int queueBufferingMaxMs = 5;
};
```

#### ConsumerConfig

```cpp
struct ConsumerConfig {
    std::string brokers;           // 必填
    std::string topic;             // 必填
    std::string groupId;           // 必填
    int32_t partition = 0;         // -1 表示所有分区
    int64_t initialOffset = -1;    // -1 表示使用 stored
    int consumeTimeoutMs = 1000;
    std::string autoOffsetReset = "earliest";
};
```

#### 配置文件加载

```cpp
ProducerConfig LoadProducerConfig(const std::string& filePath);
ConsumerConfig LoadConsumerConfig(const std::string& filePath);
```

从 JSON 文件读取配置。必填项无默认值，缺失时返回错误。

### 3. Producer.h — 生产者接口

```cpp
class Producer {
public:
    explicit Producer(const ProducerConfig& config);
    ~Producer();

    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;
    Producer(Producer&&) noexcept;
    Producer& operator=(Producer&&) noexcept;

    ErrorCode Init();
    ErrorCode Send(const std::string& payload, const std::string& key = "");
    void SetDeliveryCallback(DeliveryCallback cb);
    void Close();
};
```

- RAII：析构函数自动调用 `Close()`
- 禁止拷贝，支持移动
- `Init()` 显式初始化，返回错误码
- `Send()` 同步发送，等待投递确认

### 4. Consumer.h — 消费者接口

```cpp
class Consumer {
public:
    explicit Consumer(const ConsumerConfig& config);
    ~Consumer();

    Consumer(const Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;
    Consumer(Consumer&&) noexcept;
    Consumer& operator=(Consumer&&) noexcept;

    ErrorCode Init();
    ErrorCode Start();
    void SetMessageCallback(MessageCallback cb);
    void Stop();
    void Close();
};
```

- `Start()` 阻塞当前线程，循环消费直到 `Stop()` 被调用
- `Stop()` 线程安全，可从其他线程调用
- 用 `std::mutex` 保护 `m_running` 状态
- 消费逻辑完全由调用方在回调中实现
- 支持单分区或所有分区（`partition = -1`）

### 5. Demo (demo/main.cpp)

```cpp
#include "kafka_client/Consumer.h"
#include "kafka_client/Config.h"
#include <iostream>

int main() {
    auto config = KafkaClient::LoadConsumerConfig("config.json");

    KafkaClient::Consumer consumer(config);
    consumer.SetMessageCallback([](const KafkaClient::Message& msg) {
        std::cout << "[" << msg.partition << ":" << msg.offset << "] "
                  << msg.payload << std::endl;
    });

    if (auto err = consumer.Init(); err != KafkaClient::ErrorCode::OK) {
        std::cerr << "Init failed: " << static_cast<int>(err) << std::endl;
        return 1;
    }

    consumer.Start();
    consumer.Close();
    return 0;
}
```

## 目录结构

```
ConsoleKafkaDemoAI/
├── include/
│   └── kafka_client/
│       ├── Types.h
│       ├── Config.h
│       ├── Producer.h
│       └── Consumer.h
├── src/
│   ├── Config.cpp
│   ├── Producer.cpp
│   └── Consumer.cpp
├── demo/
│   └── main.cpp
├── config/
│   └── example.json
├── include/kafka/          # librdkafka 原有头文件（保留）
├── lib/                    # librdkafka 库文件（保留）
├── bin/                    # librdkafka DLL（保留）
└── ConsoleKafkaDemo/       # 原有代码（保留，待后续清理）
```

## 与现有代码的关系

- 原有 `ConsoleKafkaDemo/` 目录保留不动，新代码放在新的目录结构中
- librdkafka 的头文件和库文件（`include/kafka/`、`lib/`、`bin/`）保留
- 新的 `demo/main.cpp` 替代原有的 `ConsoleKafkaDemo.cpp`
- 待新代码稳定后，可选择删除旧的 `ConsoleKafkaDemo/` 目录
