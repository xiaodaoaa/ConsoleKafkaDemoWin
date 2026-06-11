# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Windows C++ console application that demonstrates Apache Kafka producer and consumer clients using the librdkafka library (C++ wrapper). Built with Visual Studio 2015 (v140 toolset), targeting Windows 8.1 SDK.

## Build

Open `ConsoleKafkaDemo.sln` in Visual Studio 2015+ and build from the IDE. Supports Win32 and x64 configurations in both Debug and Release.

Command-line build (requires VS developer environment):
```
msbuild ConsoleKafkaDemo.sln /p:Configuration=Release /p:Platform=Win32
```

The compiled executable is output to `Release\ConsoleKafkaDemo.exe`.

### Dependencies

- **librdkafka**: C and C++ Kafka client libraries. Headers in `include/kafka/`, import libs in `lib/`, runtime DLLs in `bin/` (also copied to `Release/` and `ConsoleKafkaDemo/Release/`).
- **zlib** and **libzstd**: Compression libraries (DLLs in `bin/`).

### Include/Library Paths

- Release configuration uses relative paths: `../include` and `../lib`
- Debug configuration has hardcoded absolute paths to `D:\Workspace\ConsoleKafkaDemo\include` and `D:\Workspace\ConsoleKafkaDemo\lib` -- these will need adjustment if building on a different machine.

## Architecture

Three source files, all in `ConsoleKafkaDemo/`:

- **ConsoleKafkaDemo.cpp** -- Entry point. Uses `#if 1` / `#else` to toggle between consumer mode (active) and producer mode. Consumer connects to a broker, subscribes to a topic, and polls messages. Producer reads lines from stdin and sends them to a topic (type "end" to stop).
- **KafkaProducerClient.h/.cpp** -- `KafkaProducerClient` wraps `RdKafka::Producer`. Constructor takes broker address, topic name, and partition. `Init()` configures the producer with delivery-report and event callbacks, then `Send()` produces messages synchronously (polls until the outqueue drains).
- **KafkaConsumerClient.h/.cpp** -- `KafkaConsumerClient` wraps `RdKafka::Consumer`. Constructor takes broker, topic, group ID, partition, and offset. `Init()` sets up the consumer and starts consuming from partitions 0-2 (hardcoded loop). `Start(timeout_ms)` enters a blocking consume loop, writing received message payloads to `pass.data` via a global `std::ofstream g_outfile`. `Stop()` sets `m_bRun = false` to exit the loop.

### Key librdkafka types used

- `RdKafka::Conf` for configuration (CONF_GLOBAL and CONF_TOPIC)
- `RdKafka::Producer` / `RdKafka::Consumer` for client instances
- `RdKafka::Topic` for topic handles
- `RdKafka::DeliveryReportCb` and `RdKafka::EventCb` for producer callbacks

## Switching Between Producer and Consumer

In `ConsoleKafkaDemo.cpp`, change `#if 1` to `#if 0` to switch from consumer mode to producer mode. The broker address, topic, group ID, partition, and offset are hardcoded in `main()` -- edit these to match your Kafka cluster.
