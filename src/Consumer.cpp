#include "kafka_client/Consumer.h"
#include <kafka/rdkafkacpp.h>
#include <iostream>

namespace KafkaClient {

Consumer::Consumer(const ConsumerConfig& config)
    : m_config(config) {}

Consumer::~Consumer() {
    Close();
}

Consumer::Consumer(Consumer&& other) noexcept
    : m_config(std::move(other.m_config)),
      m_consumer(other.m_consumer),
      m_topic(other.m_topic),
      m_msgCb(std::move(other.m_msgCb)),
      m_initialized(other.m_initialized),
      m_running(other.m_running),
      m_partitionsStopped(other.m_partitionsStopped) {
    other.m_consumer = nullptr;
    other.m_topic = nullptr;
    other.m_initialized = false;
    other.m_running = false;
    other.m_partitionsStopped = false;
}

Consumer& Consumer::operator=(Consumer&& other) noexcept {
    if (this != &other) {
        Close();
        m_config = std::move(other.m_config);
        m_consumer = other.m_consumer;
        m_topic = other.m_topic;
        m_msgCb = std::move(other.m_msgCb);
        m_initialized = other.m_initialized;
        m_running = other.m_running;
        m_partitionsStopped = other.m_partitionsStopped;
        other.m_consumer = nullptr;
        other.m_topic = nullptr;
        other.m_initialized = false;
        other.m_running = false;
        other.m_partitionsStopped = false;
    }
    return *this;
}

ErrorCode Consumer::Init() {
    if (m_config.brokers.empty() || m_config.topic.empty() || m_config.groupId.empty()) {
        return ErrorCode::INVALID_CONFIG;
    }

    std::string errstr;

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (!conf) {
        return ErrorCode::INVALID_CONFIG;
    }

    if (conf->set("metadata.broker.list", m_config.brokers, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "RdKafka conf set brokerlist failed: " << errstr << std::endl;
        delete conf;
        return ErrorCode::INVALID_CONFIG;
    }

    if (conf->set("group.id", m_config.groupId, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "RdKafka conf set group.id failed: " << errstr << std::endl;
        delete conf;
        return ErrorCode::INVALID_CONFIG;
    }

    if (conf->set("max.partition.fetch.bytes", "10240000", errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "RdKafka conf set max.partition.fetch.bytes failed: " << errstr << std::endl;
    }

    if (conf->set("event_cb", this, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "RdKafka conf set event_cb failed: " << errstr << std::endl;
        delete conf;
        return ErrorCode::INVALID_CONFIG;
    }

    m_consumer = RdKafka::Consumer::create(conf, errstr);
    delete conf;
    if (!m_consumer) {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        return ErrorCode::CREATE_CONSUMER_FAIL;
    }

    RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (tconf->set("auto.offset.reset", m_config.autoOffsetReset, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "RdKafka conf set auto.offset.reset failed: " << errstr << std::endl;
    }

    m_topic = RdKafka::Topic::create(m_consumer, m_config.topic, tconf, errstr);
    delete tconf;
    if (!m_topic) {
        std::cerr << "Failed to create topic: " << errstr << std::endl;
        delete m_consumer;
        m_consumer = nullptr;
        return ErrorCode::CREATE_TOPIC_FAIL;
    }

    int64_t startOffset = (m_config.initialOffset == -1)
        ? RdKafka::Topic::OFFSET_STORED
        : m_config.initialOffset;

    if (m_config.partition == -1) {
        // Consume all partitions
        for (int32_t i = 0; i < m_config.numPartitions; i++) {
            RdKafka::ErrorCode resp = m_consumer->start(m_topic, i, startOffset);
            if (resp != RdKafka::ERR_NO_ERROR) {
                std::cerr << "Failed to start consumer on partition " << i
                          << ": " << RdKafka::err2str(resp) << std::endl;
            }
        }
    } else {
        RdKafka::ErrorCode resp = m_consumer->start(m_topic, m_config.partition, startOffset);
        if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp) << std::endl;
        }
    }

    m_initialized = true;
    std::cout << "Consumer created: " << m_consumer->name() << std::endl;
    return ErrorCode::OK;
}

ErrorCode Consumer::Start() {
    if (!m_initialized) {
        return ErrorCode::NOT_INITIALIZED;
    }

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_running) {
            return ErrorCode::ALREADY_RUNNING;
        }
        m_running = true;
    }

    while (true) {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_running) break;
        }

        if (m_config.partition == -1) {
            // Consume from all partitions
            for (int32_t i = 0; i < m_config.numPartitions; i++) {
                RdKafka::Message* msg = m_consumer->consume(m_topic, i, m_config.consumeTimeoutMs);
                if (msg) {
                    ProcessMessage(msg);
                    delete msg;
                }
            }
        } else {
            RdKafka::Message* msg = m_consumer->consume(m_topic, m_config.partition, m_config.consumeTimeoutMs);
            if (msg) {
                ProcessMessage(msg);
                delete msg;
            }
        }
        m_consumer->poll(0);
    }

    if (m_config.partition == -1) {
        for (int32_t i = 0; i < m_config.numPartitions; i++) {
            m_consumer->stop(m_topic, i);
        }
    } else {
        m_consumer->stop(m_topic, m_config.partition);
    }
    m_partitionsStopped = true;
    m_consumer->poll(1000);
    return ErrorCode::OK;
}

void Consumer::event_cb(RdKafka::Event& event) {
    switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
        std::cerr << "CONSUMER ERROR: " << RdKafka::err2str(event.err())
                  << " (fatal=" << (event.fatal() ? "yes" : "no")
                  << "): " << event.str() << std::endl;
        break;
    case RdKafka::Event::EVENT_LOG:
        std::cerr << "CONSUMER LOG: " << event.str() << std::endl;
        break;
    default:
        break;
    }
}

void Consumer::SetMessageCallback(MessageCallback cb) {
    m_msgCb = std::move(cb);
}

void Consumer::Stop() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_running = false;
}

void Consumer::Close() {
    if (m_consumer && m_topic && !m_partitionsStopped) {
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
    m_partitionsStopped = false;
}

void Consumer::ProcessMessage(RdKafka::Message* msg) {
    switch (msg->err()) {
    case RdKafka::ERR__TIMED_OUT:
        break;
    case RdKafka::ERR_NO_ERROR: {
        if (m_msgCb) {
            Message km;
            km.payload = std::string(static_cast<const char*>(msg->payload()), msg->len());
            if (msg->key()) {
                km.key = *msg->key();
            }
            km.offset = msg->offset();
            km.partition = msg->partition();
            m_msgCb(km);
        }
        break;
    }
    case RdKafka::ERR__PARTITION_EOF:
        break;
    default:
        std::cerr << "Consume error: " << msg->errstr() << std::endl;
        break;
    }
}

} // namespace KafkaClient
