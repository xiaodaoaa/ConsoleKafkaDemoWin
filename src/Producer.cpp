#include "kafka_client/Producer.h"
#include <kafka/rdkafkacpp.h>
#include <iostream>

namespace KafkaClient {

Producer::Producer(const ProducerConfig& config)
    : m_config(config) {}

Producer::~Producer() {
    Close();
}

Producer::Producer(Producer&& other) noexcept
    : m_config(std::move(other.m_config)),
      m_producer(other.m_producer),
      m_topic(other.m_topic),
      m_deliveryCb(std::move(other.m_deliveryCb)),
      m_initialized(other.m_initialized) {
    other.m_producer = nullptr;
    other.m_topic = nullptr;
    other.m_initialized = false;
}

Producer& Producer::operator=(Producer&& other) noexcept {
    if (this != &other) {
        Close();
        m_config = std::move(other.m_config);
        m_producer = other.m_producer;
        m_topic = other.m_topic;
        m_deliveryCb = std::move(other.m_deliveryCb);
        m_initialized = other.m_initialized;
        other.m_producer = nullptr;
        other.m_topic = nullptr;
        other.m_initialized = false;
    }
    return *this;
}

ErrorCode Producer::Init() {
    if (m_config.brokers.empty() || m_config.topic.empty()) {
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

    if (conf->set("dr_cb", static_cast<RdKafka::DeliveryReportCb*>(this), errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "RdKafka conf set dr_cb failed: " << errstr << std::endl;
        delete conf;
        return ErrorCode::INVALID_CONFIG;
    }

    if (conf->set("event_cb", static_cast<RdKafka::EventCb*>(this), errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "RdKafka conf set event_cb failed: " << errstr << std::endl;
        delete conf;
        return ErrorCode::INVALID_CONFIG;
    }

    m_producer = RdKafka::Producer::create(conf, errstr);
    delete conf;
    if (!m_producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return ErrorCode::CREATE_PRODUCER_FAIL;
    }

    RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    m_topic = RdKafka::Topic::create(m_producer, m_config.topic, tconf, errstr);
    delete tconf;
    if (!m_topic) {
        std::cerr << "Failed to create topic: " << errstr << std::endl;
        delete m_producer;
        m_producer = nullptr;
        return ErrorCode::CREATE_TOPIC_FAIL;
    }

    m_initialized = true;
    std::cout << "Producer created: " << m_producer->name() << std::endl;
    return ErrorCode::OK;
}

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

    // Sync wait for delivery confirmation
    int wait_count = 0;
    const int max_wait = 100;
    while (wait_count < max_wait) {
        m_producer->poll(10);
        if (m_producer->outq_len() == 0) {
            break;
        }
        wait_count++;
    }

    // Trigger callback
    if (m_deliveryCb) {
        Message msg;
        msg.payload = payload;
        msg.key = key;
        ErrorCode err = (wait_count >= max_wait) ? ErrorCode::PRODUCE_FAIL : ErrorCode::OK;
        m_deliveryCb(err, msg);
    }

    return (wait_count >= max_wait) ? ErrorCode::PRODUCE_FAIL : ErrorCode::OK;
}

void Producer::SetDeliveryCallback(DeliveryCallback cb) {
    m_deliveryCb = std::move(cb);
}

void Producer::dr_cb(RdKafka::Message& message) {
    if (m_deliveryCb) {
        KafkaClient::Message msg;
        if (message.payload()) {
            msg.payload = std::string(static_cast<const char*>(message.payload()), message.len());
        }
        if (message.key()) {
            msg.key = *message.key();
        }
        msg.offset = message.offset();
        msg.partition = message.partition();
        ErrorCode err = (message.err() == RdKafka::ERR_NO_ERROR)
            ? ErrorCode::OK : ErrorCode::PRODUCE_FAIL;
        m_deliveryCb(err, msg);
    }
}

void Producer::event_cb(RdKafka::Event& event) {
    switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
        std::cerr << "PRODUCER ERROR: " << RdKafka::err2str(event.err())
                  << " (fatal=" << (event.fatal() ? "yes" : "no")
                  << "): " << event.str() << std::endl;
        break;
    case RdKafka::Event::EVENT_LOG:
        std::cerr << "PRODUCER LOG: " << event.str() << std::endl;
        break;
    default:
        break;
    }
}

void Producer::Close() {
    if (m_topic) {
        delete m_topic;
        m_topic = nullptr;
    }
    if (m_producer) {
        delete m_producer;
        m_producer = nullptr;
    }
    m_initialized = false;
}

} // namespace KafkaClient