# ConsoleKafkaDemo

Windows C++ 控制台应用，基于 librdkafka C++ 包装库演示 Apache Kafka 生产者和消费者客户端。

## 构建

使用 Visual Studio 2015（v140 工具集）打开 `ConsoleKafkaDemo.sln` 构建，支持 Win32/x64 的 Debug/Release 配置。

命令行构建（需要 VS 开发者命令提示符）：

```cmd
msbuild ConsoleKafkaDemo.sln /p:Configuration=Release /p:Platform=x86
```

输出：`bin\Release\ConsoleKafkaDemo.exe`

## 依赖

| 依赖 | 说明 | 位置 |
|------|------|------|
| librdkafka | C/C++ Kafka 客户端库 | 头文件 `include/kafka/`，导入库 `lib/`，运行时 DLL `bin/` |
| zlib / libzstd | 压缩库 | DLL 位于 `bin/` |

运行时需确保 `bin/Release/` 目录下包含所有 DLL（librdkafka.dll、librdkafkacpp.dll、zlib.dll、libzstd.dll）。

## 使用方法

```cmd
ConsoleKafkaDemo.exe consumer [config.json]
ConsoleKafkaDemo.exe producer [config.json]
```

- 不传 config.json 时默认读取 `consumer.json`（consumer 模式）或 `producer.json`（producer 模式）
- 生产者模式：输入消息内容后回车发送，输入 `quit` 退出
- 消费者模式：按 Ctrl+C 停止

## 配置

### consumer.json

```json
{
  "brokers": "localhost:9092",
  "topic": "my-topic",
  "group-id": "my-group",
  "partition": 0,
  "num.partitions": 3,
  "initial.offset": -1,
  "consume.timeout.ms": 1000,
  "auto.offset.reset": "earliest"
}
```

### producer.json

```json
{
  "brokers": "localhost:9092",
  "topic": "my-topic",
  "partition": 0,
  "queue.buffering.max.ms": 5
}
```

## 项目结构

```
include/kafka_client/  客户端封装头文件
  ├── Types.h          ErrorCode、Message、回调类型定义
  ├── Config.h         ProducerConfig/ConsumerConfig 结构体
  ├── Producer.h       Producer 类（继承 RdKafka::DeliveryReportCb/EventCb）
  └── Consumer.h       Consumer 类（继承 RdKafka::EventCb）
src/                   源码
  ├── main.cpp         入口，根据命令行参数选择 consumer/producer 模式
  ├── Producer.cpp     Producer 实现：同步发送 + delivery 回调
  ├── Consumer.cpp     Consumer 实现：分区消费循环 + 异步停止
  └── Config.cpp       简易 JSON 配置文件解析
```

## 已知风险

- JSON 解析为手写实现，不支持转义引号、嵌套对象、浮点数
- Consumer 的 all-partitions 模式为忙轮询，无休眠
- 无单元测试
- Debug 配置的包含路径为硬编码绝对路径