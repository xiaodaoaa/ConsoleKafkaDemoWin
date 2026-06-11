#include "kafka_client/Consumer.h"
#include "kafka_client/Producer.h"
#include "kafka_client/Config.h"
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " [consumer|producer] [config.json]" << std::endl;
        return 1;
    }

    std::string mode = argv[1];
    std::string configFile = (argc >= 3) ? argv[2] : 
        (mode == "consumer" ? "consumer.json" : "producer.json");

    if (mode == "consumer") {
        KafkaClient::ConsumerConfig config = KafkaClient::LoadConsumerConfig(configFile);
        if (config.brokers.empty() || config.topic.empty() || config.groupId.empty()) {
            std::cerr << "Invalid consumer config" << std::endl;
            return 1;
        }

        KafkaClient::Consumer consumer(config);
        consumer.SetMessageCallback([](const KafkaClient::Message& msg) {
            std::cout << "[" << msg.partition << ":" << msg.offset << "] "
                      << msg.payload << std::endl;
        });

        KafkaClient::ErrorCode err = consumer.Init();
        if (err != KafkaClient::ErrorCode::OK) {
            std::cerr << "Init failed: " << static_cast<int>(err) << std::endl;
            return 1;
        }

        std::cout << "Consumer started, press Ctrl+C to stop..." << std::endl;
        consumer.Start();
        consumer.Close();
    }
    else if (mode == "producer") {
        KafkaClient::ProducerConfig config = KafkaClient::LoadProducerConfig(configFile);
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

        KafkaClient::ErrorCode err = producer.Init();
        if (err != KafkaClient::ErrorCode::OK) {
            std::cerr << "Init failed: " << static_cast<int>(err) << std::endl;
            return 1;
        }

        std::cout << "Producer ready. Enter messages (type 'quit' to exit):" << std::endl;
        std::string line;
        while (true) {
            std::getline(std::cin, line);
            if (line == "quit") break;
            if (line.empty()) continue;

            err = producer.Send(line);
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