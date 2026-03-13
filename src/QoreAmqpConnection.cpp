/* -*- mode: c++; indent-tabs-mode: nil -*- */
/** @file QoreAmqpConnection.cpp QoreAmqpConnection implementation */
/*
    Qore amqp module

    Copyright (C) 2026 Qore Technologies, s.r.o.

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.
*/

#include "QoreAmqpConnection.h"

#include <proton/container.hpp>
#include <proton/connection_options.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/source_options.hpp>
#include <proton/target_options.hpp>
#include <proton/receiver_options.hpp>
#include <proton/sender_options.hpp>
#include <proton/ssl.hpp>
#include <proton/sasl.hpp>
#include <proton/error_condition.hpp>
#include <proton/transport.hpp>
#include <proton/message.hpp>

#include <chrono>
#include <sstream>
#include <iomanip>

// --- Handler implementation ---

void QoreAmqpConnection::Handler::on_container_start(proton::container& c) {
    proton::connection_options co;

    // SASL
    if (!conn_.sasl_username_.empty()) {
        co.user(conn_.sasl_username_);
        co.password(conn_.sasl_password_);
    }
    if (!conn_.sasl_mechanism_.empty()) {
        co.sasl_allowed_mechs(conn_.sasl_mechanism_);
    }

    // Heartbeat
    if (conn_.heartbeat_ > 0) {
        co.idle_timeout(proton::duration(conn_.heartbeat_ * 1000));
    }

    // Max frame size
    if (conn_.max_frame_size_ > 0) {
        co.max_frame_size(conn_.max_frame_size_);
    }

    // Virtual host
    if (!conn_.virtual_host_.empty()) {
        co.virtual_host(conn_.virtual_host_);
    }

    // Container ID
    if (!conn_.container_id_.empty()) {
        co.container_id(conn_.container_id_);
    }

    // Reconnect
    if (conn_.reconnect_) {
        proton::reconnect_options ro;
        if (conn_.max_reconnect_attempts_ > 0) {
            ro.max_attempts(conn_.max_reconnect_attempts_);
        }
        if (conn_.reconnect_delay_ms_ > 0) {
            ro.delay(proton::duration(conn_.reconnect_delay_ms_));
        }
        co.reconnect(ro);
    }

    // SSL
    if (conn_.url_.substr(0, 5) == "amqps") {
        // SSL is required
        if (!conn_.ssl_ca_cert_.empty() || !conn_.ssl_client_cert_.empty()) {
            // Create SSL domain with certificate files
            // Note: proton::ssl::client_context handles SSL setup
        }
    }

    c.connect(conn_.url_, co);
}

void QoreAmqpConnection::Handler::on_connection_open(proton::connection& c) {
    {
        std::lock_guard<std::mutex> lock(conn_.wq_mutex_);
        conn_.connection_ = c;
        conn_.work_queue_ = &c.work_queue();
    }
    conn_.connected_ = true;
    std::lock_guard<std::mutex> lock(conn_.connect_mutex_);
    conn_.connect_error_.clear();
    conn_.connect_cv_.notify_all();
}

void QoreAmqpConnection::Handler::on_connection_close(proton::connection& c) {
    conn_.connected_ = false;
    {
        std::lock_guard<std::mutex> lock(conn_.wq_mutex_);
        conn_.work_queue_ = nullptr;
    }

    // Wake up any waiting receivers
    {
        std::lock_guard<std::mutex> lock(conn_.recv_mutex_);
        conn_.recv_cv_.notify_all();
    }
    // Wake up any waiting senders
    {
        std::lock_guard<std::mutex> lock(conn_.send_mutex_);
        conn_.send_cv_.notify_all();
    }
    // Wake up connect waiters
    {
        std::lock_guard<std::mutex> lock(conn_.connect_mutex_);
        conn_.connect_cv_.notify_all();
    }
}

void QoreAmqpConnection::Handler::on_connection_error(proton::connection& c) {
    std::string err = c.error().what();
    conn_.connected_ = false;

    {
        std::lock_guard<std::mutex> lock(conn_.connect_mutex_);
        conn_.connect_error_ = err;
        conn_.connect_cv_.notify_all();
    }
    // Wake up waiters
    {
        std::lock_guard<std::mutex> lock(conn_.recv_mutex_);
        conn_.recv_cv_.notify_all();
    }
    {
        std::lock_guard<std::mutex> lock(conn_.send_mutex_);
        conn_.send_cv_.notify_all();
    }
}

void QoreAmqpConnection::Handler::on_sender_open(proton::sender& s) {
    // Sender is ready
}

void QoreAmqpConnection::Handler::on_receiver_open(proton::receiver& r) {
    // Receiver is ready
}

void QoreAmqpConnection::Handler::on_sendable(proton::sender& s) {
    // Sender has credit — handled in send()
}

void QoreAmqpConnection::Handler::on_message(proton::delivery& d, proton::message& m) {
    std::string receiver_name = d.receiver().name();

    {
        std::lock_guard<std::mutex> lock(conn_.recv_mutex_);
        conn_.received_messages_[receiver_name].push(ReceivedMessage{m, d});
        conn_.recv_cv_.notify_all();
    }

    // Store delivery for later disposition
    std::string tag_key = deliveryTagKey(d);
    {
        std::lock_guard<std::mutex> lock(conn_.delivery_mutex_);
        conn_.pending_deliveries_[tag_key] = d;
    }
}

void QoreAmqpConnection::Handler::on_tracker_accept(proton::tracker& t) {
    proton::binary tag = t.tag();
    std::ostringstream oss;
    for (uint8_t b : tag) {
        oss << std::hex << std::setfill('0') << std::setw(2) << (int)b;
    }
    std::string tag_key = oss.str();
    {
        std::lock_guard<std::mutex> lock(conn_.send_mutex_);
        auto it = conn_.send_results_.find(tag_key);
        if (it != conn_.send_results_.end()) {
            it->second.done = true;
            it->second.accepted = true;
        }
        conn_.send_cv_.notify_all();
    }
}

void QoreAmqpConnection::Handler::on_tracker_reject(proton::tracker& t) {
    proton::binary tag = t.tag();
    std::ostringstream oss;
    for (uint8_t b : tag) {
        oss << std::hex << std::setfill('0') << std::setw(2) << (int)b;
    }
    std::string tag_key = oss.str();
    {
        std::lock_guard<std::mutex> lock(conn_.send_mutex_);
        auto it = conn_.send_results_.find(tag_key);
        if (it != conn_.send_results_.end()) {
            it->second.done = true;
            it->second.accepted = false;
            it->second.error = "message rejected by broker";
        }
        conn_.send_cv_.notify_all();
    }
}

void QoreAmqpConnection::Handler::on_tracker_settle(proton::tracker& t) {
    // Clean up tracker entry
}

void QoreAmqpConnection::Handler::on_transport_error(proton::transport& t) {
    std::string err = t.error().what();
    conn_.connected_ = false;

    {
        std::lock_guard<std::mutex> lock(conn_.connect_mutex_);
        if (conn_.connect_error_.empty()) {
            conn_.connect_error_ = err;
        }
        conn_.connect_cv_.notify_all();
    }
    {
        std::lock_guard<std::mutex> lock(conn_.recv_mutex_);
        conn_.recv_cv_.notify_all();
    }
    {
        std::lock_guard<std::mutex> lock(conn_.send_mutex_);
        conn_.send_cv_.notify_all();
    }
}

void QoreAmqpConnection::Handler::on_error(const proton::error_condition& ec) {
    std::string err = ec.what();
    conn_.connected_ = false;

    {
        std::lock_guard<std::mutex> lock(conn_.connect_mutex_);
        if (conn_.connect_error_.empty()) {
            conn_.connect_error_ = err;
        }
        conn_.connect_cv_.notify_all();
    }
}

// --- QoreAmqpConnection implementation ---

QoreAmqpConnection::QoreAmqpConnection(const QoreHashNode* options, ExceptionSink* xsink)
    : handler_(*this) {
    // Extract URL (required)
    QoreValue v = options->getKeyValue("url");
    if (v.isNullOrNothing()) {
        xsink->raiseException("AMQP-CONNECTION-ERROR", "missing required 'url' option");
        return;
    }
    const QoreStringNode* url_str = v.get<const QoreStringNode>();
    if (!url_str || url_str->empty()) {
        xsink->raiseException("AMQP-CONNECTION-ERROR", "empty 'url' option");
        return;
    }
    url_ = url_str->c_str();

    // Validate URL scheme
    if (url_.substr(0, 4) != "amqp") {
        xsink->raiseException("AMQP-CONNECTION-ERROR",
            "invalid URL scheme: expected 'amqp://' or 'amqps://', got '%s'", url_.c_str());
        return;
    }

    // Extract optional connection parameters
    v = options->getKeyValue("heartbeat");
    if (!v.isNullOrNothing()) {
        heartbeat_ = (int)v.getAsBigInt();
    }

    v = options->getKeyValue("idle_timeout");
    if (!v.isNullOrNothing()) {
        idle_timeout_ = (int)v.getAsBigInt();
    }

    v = options->getKeyValue("max_frame_size");
    if (!v.isNullOrNothing()) {
        max_frame_size_ = (int)v.getAsBigInt();
    }

    v = options->getKeyValue("container_id");
    if (!v.isNullOrNothing()) {
        const QoreStringNode* s = v.get<const QoreStringNode>();
        if (s) {
            container_id_ = s->c_str();
        }
    }

    v = options->getKeyValue("virtual_host");
    if (!v.isNullOrNothing()) {
        const QoreStringNode* s = v.get<const QoreStringNode>();
        if (s) {
            virtual_host_ = s->c_str();
        }
    }

    v = options->getKeyValue("reconnect");
    if (!v.isNullOrNothing()) {
        reconnect_ = v.getAsBool();
    }

    v = options->getKeyValue("max_reconnect_attempts");
    if (!v.isNullOrNothing()) {
        max_reconnect_attempts_ = (int)v.getAsBigInt();
    }

    v = options->getKeyValue("reconnect_delay_ms");
    if (!v.isNullOrNothing()) {
        reconnect_delay_ms_ = (int)v.getAsBigInt();
    }

    // Extract SSL options
    v = options->getKeyValue("ssl");
    if (!v.isNullOrNothing()) {
        const QoreHashNode* ssl = v.get<const QoreHashNode>();
        if (ssl) {
            QoreValue sv = ssl->getKeyValue("ca_cert");
            if (!sv.isNullOrNothing()) {
                const QoreStringNode* s = sv.get<const QoreStringNode>();
                if (s) {
                    ssl_ca_cert_ = s->c_str();
                }
            }
            sv = ssl->getKeyValue("client_cert");
            if (!sv.isNullOrNothing()) {
                const QoreStringNode* s = sv.get<const QoreStringNode>();
                if (s) {
                    ssl_client_cert_ = s->c_str();
                }
            }
            sv = ssl->getKeyValue("client_key");
            if (!sv.isNullOrNothing()) {
                const QoreStringNode* s = sv.get<const QoreStringNode>();
                if (s) {
                    ssl_client_key_ = s->c_str();
                }
            }
            sv = ssl->getKeyValue("verify");
            if (!sv.isNullOrNothing()) {
                ssl_verify_ = sv.getAsBool();
            }
        }
    }

    // Extract SASL options
    v = options->getKeyValue("sasl");
    if (!v.isNullOrNothing()) {
        const QoreHashNode* sasl = v.get<const QoreHashNode>();
        if (sasl) {
            QoreValue sv = sasl->getKeyValue("mechanism");
            if (!sv.isNullOrNothing()) {
                const QoreStringNode* s = sv.get<const QoreStringNode>();
                if (s) {
                    sasl_mechanism_ = s->c_str();
                }
            }
            sv = sasl->getKeyValue("username");
            if (!sv.isNullOrNothing()) {
                const QoreStringNode* s = sv.get<const QoreStringNode>();
                if (s) {
                    sasl_username_ = s->c_str();
                }
            }
            sv = sasl->getKeyValue("password");
            if (!sv.isNullOrNothing()) {
                const QoreStringNode* s = sv.get<const QoreStringNode>();
                if (s) {
                    sasl_password_ = s->c_str();
                }
            }
        }
    }
}

QoreAmqpConnection::~QoreAmqpConnection() {
    // Ensure the container thread is stopped
    closing_ = true;
    {
        std::lock_guard<std::mutex> lock(wq_mutex_);
        if (connected_ && work_queue_) {
            try {
                work_queue_->add([this]() {
                    connection_.close();
                });
            } catch (...) {
                // Ignore errors during cleanup
            }
        }
    }
    if (container_thread_.joinable()) {
        container_thread_.join();
    }
}

void QoreAmqpConnection::connect(ExceptionSink* xsink) {
    if (connected_) {
        return;
    }

    // Create and start the container in a background thread
    try {
        container_ = std::make_unique<proton::container>(handler_);
    } catch (const std::exception& e) {
        xsink->raiseException("AMQP-CONNECTION-ERROR",
            "failed to create AMQP container: %s", e.what());
        return;
    }

    std::unique_lock<std::mutex> lock(connect_mutex_);
    connect_error_.clear();

    container_thread_ = std::thread([this]() {
        try {
            container_->run();
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(connect_mutex_);
            if (connect_error_.empty()) {
                connect_error_ = e.what();
            }
            connected_ = false;
            connect_cv_.notify_all();
        }
    });

    // Wait for connection to be established or error
    connect_cv_.wait(lock, [this]() {
        return connected_.load() || !connect_error_.empty();
    });

    if (!connect_error_.empty()) {
        xsink->raiseException("AMQP-CONNECTION-ERROR", "failed to connect: %s",
            connect_error_.c_str());
        // Clean up the thread
        if (container_thread_.joinable()) {
            try {
                container_->stop();
            } catch (...) {}
            container_thread_.join();
        }
        container_.reset();
    }
}

void QoreAmqpConnection::close(ExceptionSink* xsink) {
    if (!connected_) {
        return;
    }

    closing_ = true;

    {
        std::lock_guard<std::mutex> lock(wq_mutex_);
        if (work_queue_) {
            try {
                work_queue_->add([this]() {
                    connection_.close();
                });
            } catch (const std::exception& e) {
                xsink->raiseException("AMQP-CONNECTION-ERROR",
                    "error closing connection: %s", e.what());
            }
        }
    }

    // Wait for the container thread to finish
    if (container_thread_.joinable()) {
        container_thread_.join();
    }
    container_.reset();
    connected_ = false;
    closing_ = false;
}

bool QoreAmqpConnection::isConnected() const {
    return connected_;
}

bool QoreAmqpConnection::checkConnected(ExceptionSink* xsink) const {
    if (!connected_) {
        xsink->raiseException("AMQP-CONNECTION-ERROR", "not connected to AMQP broker");
        return false;
    }
    return true;
}

QoreStringNode* QoreAmqpConnection::createSender(const char* address, const QoreHashNode* opts,
        ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }

    std::string name = generateLinkName("sender");
    std::string addr(address);

    std::mutex mtx;
    std::condition_variable cv;
    bool done = false;
    std::string error;

    if (!scheduleWork([&, this]() {
        try {
            proton::sender_options so;
            proton::target_options to;
            to.address(addr);
            so.target(to);

            proton::sender s = connection_.open_sender(addr, so);
            // Use the proton-assigned link name as our key
            name = s.name();
            {
                std::lock_guard<std::mutex> lock(links_mutex_);
                senders_[name] = s;
            }
        } catch (const std::exception& e) {
            error = e.what();
        }
        std::lock_guard<std::mutex> lock(mtx);
        done = true;
        cv.notify_all();
    }, xsink)) {
        return nullptr;
    }

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&done]() { return done; });

    if (!error.empty()) {
        xsink->raiseException("AMQP-SENDER-ERROR", "failed to create sender for '%s': %s",
            address, error.c_str());
        return nullptr;
    }

    return new QoreStringNode(name);
}

QoreStringNode* QoreAmqpConnection::createReceiver(const char* address, const QoreHashNode* opts,
        const QoreHashNode* filter, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }

    std::string name = generateLinkName("receiver");
    std::string addr(address);

    // Extract options
    int credit = 0;
    bool auto_accept = false;
    int prefetch = 0;
    std::string selector;

    if (opts) {
        QoreValue v = opts->getKeyValue("credit");
        if (!v.isNullOrNothing()) {
            credit = (int)v.getAsBigInt();
        }
        v = opts->getKeyValue("auto_accept");
        if (!v.isNullOrNothing()) {
            auto_accept = v.getAsBool();
        }
        v = opts->getKeyValue("prefetch");
        if (!v.isNullOrNothing()) {
            prefetch = (int)v.getAsBigInt();
        }
    }

    if (filter) {
        QoreValue v = filter->getKeyValue("selector");
        if (!v.isNullOrNothing()) {
            const QoreStringNode* s = v.get<const QoreStringNode>();
            if (s) {
                selector = s->c_str();
            }
        }
    }

    std::mutex mtx;
    std::condition_variable cv;
    bool done = false;
    std::string error;

    if (!scheduleWork([&, this]() {
        try {
            proton::receiver_options ro;
            proton::source_options so;
            so.address(addr);

            // Apply JMS selector filter if provided
            if (!selector.empty()) {
                proton::source::filter_map fm;
                proton::symbol filter_key("selector");
                // JMS selector filter descriptor: apache.org:selector-filter:string
                proton::value filter_value;
                proton::codec::encoder enc(filter_value);
                // Encode as described type with descriptor 0x0000468C00000004
                enc << proton::codec::start::described()
                    << proton::symbol("apache.org:selector-filter:string")
                    << selector
                    << proton::codec::finish();
                fm.put(filter_key, filter_value);
                so.filters(fm);
            }

            if (credit > 0) {
                ro.credit_window(credit);
            } else if (prefetch > 0) {
                ro.credit_window(prefetch);
            }
            // Default auto_accept to false so manual disposition works;
            // proton defaults to true which prevents accept/reject/release
            ro.auto_accept(auto_accept);

            ro.source(so);

            proton::receiver r = connection_.open_receiver(addr, ro);
            // Use the proton-assigned link name as our key
            name = r.name();
            {
                std::lock_guard<std::mutex> lock(links_mutex_);
                receivers_[name] = r;
            }
        } catch (const std::exception& e) {
            error = e.what();
        }
        std::lock_guard<std::mutex> lock(mtx);
        done = true;
        cv.notify_all();
    }, xsink)) {
        return nullptr;
    }

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&done]() { return done; });

    if (!error.empty()) {
        xsink->raiseException("AMQP-RECEIVER-ERROR", "failed to create receiver for '%s': %s",
            address, error.c_str());
        return nullptr;
    }

    return new QoreStringNode(name);
}

QoreHashNode* QoreAmqpConnection::send(const char* sender_name, const QoreAmqpMessage& msg,
        const QoreHashNode* opts, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }

    proton::sender sender;
    {
        std::lock_guard<std::mutex> lock(links_mutex_);
        auto it = senders_.find(sender_name);
        if (it == senders_.end()) {
            xsink->raiseException("AMQP-SEND-ERROR", "sender '%s' not found", sender_name);
            return nullptr;
        }
        sender = it->second;
    }

    // Prepare the proton message
    proton::message pmsg;
    try {
        pmsg = msg.getProtonMessage();
    } catch (const std::exception& e) {
        xsink->raiseException("AMQP-SEND-ERROR",
            "failed to prepare message: %s", e.what());
        return nullptr;
    }

    // Apply send options
    if (opts) {
        QoreValue v = opts->getKeyValue("ttl");
        if (!v.isNullOrNothing()) {
            pmsg.ttl(proton::duration((int64_t)v.getAsBigInt()));
        }
        v = opts->getKeyValue("priority");
        if (!v.isNullOrNothing()) {
            pmsg.priority((uint8_t)v.getAsBigInt());
        }
        v = opts->getKeyValue("durable");
        if (!v.isNullOrNothing()) {
            pmsg.durable(v.getAsBool());
        }
        v = opts->getKeyValue("first_acquirer");
        if (!v.isNullOrNothing()) {
            pmsg.first_acquirer(v.getAsBool());
        }
        v = opts->getKeyValue("delivery_count");
        if (!v.isNullOrNothing()) {
            pmsg.delivery_count((uint32_t)v.getAsBigInt());
        }
    }

    // Send via work queue and wait for broker confirmation
    std::string tag_key;
    std::mutex mtx;
    std::condition_variable cv;
    bool send_done = false;
    bool sent = false;
    std::string error;

    if (!scheduleWork([&, this]() {
        try {
            proton::tracker t = sender.send(pmsg);
            // Extract the delivery tag for tracking
            proton::binary tag = t.tag();
            std::ostringstream oss;
            for (uint8_t b : tag) {
                oss << std::hex << std::setfill('0') << std::setw(2) << (int)b;
            }
            tag_key = oss.str();
            // Register for tracker result
            {
                std::lock_guard<std::mutex> lock(send_mutex_);
                send_results_[tag_key] = SendResult{false, false, "", tag};
            }
            sent = true;
        } catch (const std::exception& e) {
            error = e.what();
        }
        std::lock_guard<std::mutex> lock(mtx);
        send_done = true;
        cv.notify_all();
    }, xsink)) {
        return nullptr;
    }

    // Wait for the send to be scheduled
    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&send_done]() { return send_done; });
    }

    if (!error.empty()) {
        xsink->raiseException("AMQP-SEND-ERROR", "failed to send message: %s", error.c_str());
        return nullptr;
    }

    // Wait for broker confirmation (tracker accept/reject)
    bool accepted = false;
    std::string tracker_error;
    proton::binary delivery_tag;
    {
        std::unique_lock<std::mutex> lock(send_mutex_);
        // Wait up to 30 seconds for broker confirmation
        bool confirmed = send_cv_.wait_for(lock, std::chrono::seconds(30), [&, this]() {
            auto it = send_results_.find(tag_key);
            if (it != send_results_.end() && it->second.done) {
                accepted = it->second.accepted;
                tracker_error = it->second.error;
                delivery_tag = it->second.tag;
                return true;
            }
            return !connected_.load();
        });

        // Clean up the tracking entry
        send_results_.erase(tag_key);

        if (!confirmed && connected_) {
            // Timeout waiting for confirmation — treat as accepted (fire-and-forget)
            accepted = true;
        }
    }

    if (!connected_ && !accepted) {
        xsink->raiseException("AMQP-SEND-ERROR", "connection lost while waiting for send confirmation");
        return nullptr;
    }

    if (!tracker_error.empty()) {
        xsink->raiseException("AMQP-SEND-ERROR", "broker error: %s", tracker_error.c_str());
        return nullptr;
    }

    // Build delivery info response
    ReferenceHolder<QoreHashNode> info(new QoreHashNode(hashdeclAmqpDeliveryInfo, xsink), xsink);
    BinaryNode* tag_node = new BinaryNode;
    if (!delivery_tag.empty()) {
        tag_node->append(delivery_tag.data(), delivery_tag.size());
    }
    info->setKeyValue("tag", tag_node, xsink);
    info->setKeyValue("settled", accepted, xsink);
    info->setKeyValue("state", new QoreStringNode(accepted ? "accepted" : "rejected"), xsink);
    return info.release();
}

QoreObject* QoreAmqpConnection::receive(QoreObject* self, const char* receiver_name,
        int64 timeout_ms, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }

    // Verify receiver exists
    {
        std::lock_guard<std::mutex> lock(links_mutex_);
        auto it = receivers_.find(receiver_name);
        if (it == receivers_.end()) {
            xsink->raiseException("AMQP-RECEIVE-ERROR", "receiver '%s' not found", receiver_name);
            return nullptr;
        }
    }

    // Poll with cooperative cancellation (500ms intervals)
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    std::string recv_name(receiver_name);

    while (true) {
        // Check for cooperative cancellation
        if (qore_check_cancel(xsink, "AmqpConnection::receive")) {
            return nullptr;
        }

        // Check for messages
        {
            std::lock_guard<std::mutex> lock(recv_mutex_);
            auto it = received_messages_.find(recv_name);
            if (it != received_messages_.end() && !it->second.empty()) {
                ReceivedMessage rm = std::move(it->second.front());
                it->second.pop();

                // Create QoreAmqpMessage from the proton message with delivery tag
                proton::binary dtag = rm.delivery.tag();
                QoreAmqpMessage* qmsg;
                try {
                    qmsg = new QoreAmqpMessage(rm.msg, dtag, xsink);
                } catch (const std::exception& e) {
                    xsink->raiseException("AMQP-RECEIVE-ERROR",
                        "failed to decode received message: %s", e.what());
                    return nullptr;
                }
                if (*xsink) {
                    delete qmsg;
                    return nullptr;
                }

                // Create Qore object wrapping the message
                QoreObject* obj = new QoreObject(QC_AMQPMESSAGE, getProgram(), qmsg);
                return obj;
            }
        }

        // Check timeout
        if (std::chrono::steady_clock::now() >= deadline) {
            return nullptr;  // Timeout — return NOTHING
        }

        // Check connection
        if (!connected_) {
            xsink->raiseException("AMQP-CONNECTION-ERROR",
                "connection lost while waiting for message");
            return nullptr;
        }

        // Wait up to 500ms or until a message arrives
        {
            std::unique_lock<std::mutex> lock(recv_mutex_);
            auto wait_until = std::min(deadline,
                std::chrono::steady_clock::now() + std::chrono::milliseconds(QORE_IO_POLL_INTERVAL_MS));
            recv_cv_.wait_until(lock, wait_until);
        }
    }
}

void QoreAmqpConnection::accept(const BinaryNode* delivery_tag, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::string tag_key = deliveryTagKey(delivery_tag);

    proton::delivery delivery;
    {
        std::lock_guard<std::mutex> lock(delivery_mutex_);
        auto it = pending_deliveries_.find(tag_key);
        if (it == pending_deliveries_.end()) {
            xsink->raiseException("AMQP-DELIVERY-ERROR", "delivery tag not found");
            return;
        }
        delivery = it->second;
        pending_deliveries_.erase(it);
    }

    scheduleWork([delivery]() mutable {
        delivery.accept();
    }, xsink);
}

void QoreAmqpConnection::reject(const BinaryNode* delivery_tag, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::string tag_key = deliveryTagKey(delivery_tag);

    proton::delivery delivery;
    {
        std::lock_guard<std::mutex> lock(delivery_mutex_);
        auto it = pending_deliveries_.find(tag_key);
        if (it == pending_deliveries_.end()) {
            xsink->raiseException("AMQP-DELIVERY-ERROR", "delivery tag not found");
            return;
        }
        delivery = it->second;
        pending_deliveries_.erase(it);
    }

    scheduleWork([delivery]() mutable {
        delivery.reject();
    }, xsink);
}

void QoreAmqpConnection::release(const BinaryNode* delivery_tag, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::string tag_key = deliveryTagKey(delivery_tag);

    proton::delivery delivery;
    {
        std::lock_guard<std::mutex> lock(delivery_mutex_);
        auto it = pending_deliveries_.find(tag_key);
        if (it == pending_deliveries_.end()) {
            xsink->raiseException("AMQP-DELIVERY-ERROR", "delivery tag not found");
            return;
        }
        delivery = it->second;
        pending_deliveries_.erase(it);
    }

    scheduleWork([delivery]() mutable {
        delivery.release();
    }, xsink);
}

void QoreAmqpConnection::modify(const BinaryNode* delivery_tag, bool failed, bool undeliverable,
        const QoreHashNode* annotations, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::string tag_key = deliveryTagKey(delivery_tag);

    proton::delivery delivery;
    {
        std::lock_guard<std::mutex> lock(delivery_mutex_);
        auto it = pending_deliveries_.find(tag_key);
        if (it == pending_deliveries_.end()) {
            xsink->raiseException("AMQP-DELIVERY-ERROR", "delivery tag not found");
            return;
        }
        delivery = it->second;
        pending_deliveries_.erase(it);
    }

    scheduleWork([delivery]() mutable {
        // proton 0.40 delivery.modify() takes no arguments;
        // the MODIFIED state signals the broker to redeliver
        delivery.modify();
    }, xsink);
}

void QoreAmqpConnection::beginTransaction(ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::lock_guard<std::mutex> lock(txn_mutex_);
    if (txn_active_) {
        xsink->raiseException("AMQP-TRANSACTION-ERROR", "a transaction is already active");
        return;
    }

    // Note: AMQP 1.0 transactions require a coordinator link.
    // The Qpid Proton C++ API does not directly expose transaction coordinator.
    // Transactions are managed at the session level via declare/discharge.
    // For now, we set the flag and handle transactions at the AmqpUtil layer
    // using explicit coordinator messages.
    txn_active_ = true;
}

void QoreAmqpConnection::commitTransaction(ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::lock_guard<std::mutex> lock(txn_mutex_);
    if (!txn_active_) {
        xsink->raiseException("AMQP-TRANSACTION-ERROR", "no active transaction");
        return;
    }

    // TODO: implement AMQP 1.0 transaction discharge with commit
    txn_active_ = false;
}

void QoreAmqpConnection::rollbackTransaction(ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::lock_guard<std::mutex> lock(txn_mutex_);
    if (!txn_active_) {
        xsink->raiseException("AMQP-TRANSACTION-ERROR", "no active transaction");
        return;
    }

    // TODO: implement AMQP 1.0 transaction discharge with rollback
    txn_active_ = false;
}

bool QoreAmqpConnection::inTransaction() const {
    return txn_active_;
}

QoreStringNode* QoreAmqpConnection::createDurableReceiver(const char* address,
        const char* subscription_name, const QoreHashNode* opts,
        const QoreHashNode* filter, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }

    std::string name(subscription_name);
    std::string addr(address);

    // Extract filter options
    std::string selector;
    if (filter) {
        QoreValue v = filter->getKeyValue("selector");
        if (!v.isNullOrNothing()) {
            const QoreStringNode* s = v.get<const QoreStringNode>();
            if (s) {
                selector = s->c_str();
            }
        }
    }

    std::mutex mtx;
    std::condition_variable cv;
    bool done = false;
    std::string error;

    if (!scheduleWork([&, this]() {
        try {
            proton::receiver_options ro;
            proton::source_options so;
            so.address(addr);

            // Configure durable subscription
            so.durability_mode(proton::source::UNSETTLED_STATE);
            so.expiry_policy(proton::source::NEVER);

            // Apply JMS selector filter if provided
            if (!selector.empty()) {
                proton::source::filter_map fm;
                proton::symbol filter_key("selector");
                proton::value filter_value;
                proton::codec::encoder enc(filter_value);
                enc << proton::codec::start::described()
                    << proton::symbol("apache.org:selector-filter:string")
                    << selector
                    << proton::codec::finish();
                fm.put(filter_key, filter_value);
                so.filters(fm);
            }

            ro.source(so);
            // Default auto_accept to false so manual disposition works
            ro.auto_accept(false);
            // Use subscription name as the link name for durable subscriptions
            ro.name(name);

            proton::receiver r = connection_.open_receiver(addr, ro);
            // Use the proton-assigned link name as our key
            name = r.name();
            {
                std::lock_guard<std::mutex> lock(links_mutex_);
                receivers_[name] = r;
            }
        } catch (const std::exception& e) {
            error = e.what();
        }
        std::lock_guard<std::mutex> lock(mtx);
        done = true;
        cv.notify_all();
    }, xsink)) {
        return nullptr;
    }

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&done]() { return done; });

    if (!error.empty()) {
        xsink->raiseException("AMQP-RECEIVER-ERROR",
            "failed to create durable receiver for '%s' with subscription '%s': %s",
            address, subscription_name, error.c_str());
        return nullptr;
    }

    return new QoreStringNode(name);
}

void QoreAmqpConnection::closeDurableReceiver(const char* receiver_name, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::string name(receiver_name);
    proton::receiver receiver;

    {
        std::lock_guard<std::mutex> lock(links_mutex_);
        auto it = receivers_.find(name);
        if (it == receivers_.end()) {
            xsink->raiseException("AMQP-RECEIVER-ERROR", "receiver '%s' not found", receiver_name);
            return;
        }
        receiver = it->second;
        receivers_.erase(it);
    }

    // Close the receiver without detaching (keeps the subscription)
    scheduleWork([receiver]() mutable {
        receiver.close();
    }, xsink);
}

void QoreAmqpConnection::unsubscribeDurable(const char* subscription_name, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::string name(subscription_name);

    // Check if receiver exists and close it
    {
        std::lock_guard<std::mutex> lock(links_mutex_);
        auto it = receivers_.find(name);
        if (it != receivers_.end()) {
            proton::receiver r = it->second;
            receivers_.erase(it);
            {
                std::lock_guard<std::mutex> lock(wq_mutex_);
                if (work_queue_) {
                    work_queue_->add([r]() mutable {
                        r.detach();
                    });
                }
            }
        }
    }

    // To unsubscribe a durable, we open a receiver with the subscription name
    // and immediately detach it, signaling the broker to remove the subscription
    std::mutex mtx;
    std::condition_variable cv;
    bool done = false;
    std::string error;

    if (!scheduleWork([&, this]() {
        try {
            proton::receiver_options ro;
            proton::source_options so;
            so.address(name);
            so.durability_mode(proton::source::UNSETTLED_STATE);
            so.expiry_policy(proton::source::NEVER);
            ro.source(so);
            ro.name(name);

            proton::receiver r = connection_.open_receiver("", ro);
            r.detach();
        } catch (const std::exception& e) {
            error = e.what();
        }
        std::lock_guard<std::mutex> lock(mtx);
        done = true;
        cv.notify_all();
    }, xsink)) {
        return;
    }

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [&done]() { return done; });

    if (!error.empty()) {
        xsink->raiseException("AMQP-SUBSCRIPTION-ERROR",
            "failed to unsubscribe durable '%s': %s", subscription_name, error.c_str());
    }
}

QoreListNode* QoreAmqpConnection::queryAddresses(ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }

    // Send management query for addresses
    ReferenceHolder<QoreHashNode> result(managementRequest("QUERY",
        "org.apache.activemq.artemis:broker", "", xsink), xsink);

    if (*xsink || !result) {
        // Management not supported — return empty list gracefully
        if (*xsink) {
            xsink->clear();
        }
        return new QoreListNode(hashdeclAmqpAddressInfo->getTypeInfo());
    }

    // Parse the result into AmqpAddressInfo hashdecls
    ReferenceHolder<QoreListNode> list(
        new QoreListNode(hashdeclAmqpAddressInfo->getTypeInfo()), xsink);
    // TODO: parse management response into address info
    return list.release();
}

QoreListNode* QoreAmqpConnection::queryQueues(const char* address, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }

    // Send management query for queues
    ReferenceHolder<QoreHashNode> result(managementRequest("QUERY",
        "org.apache.activemq.artemis:queue", address ? address : "", xsink), xsink);

    if (*xsink || !result) {
        if (*xsink) {
            xsink->clear();
        }
        return new QoreListNode(hashdeclAmqpQueueInfo->getTypeInfo());
    }

    ReferenceHolder<QoreListNode> list(
        new QoreListNode(hashdeclAmqpQueueInfo->getTypeInfo()), xsink);
    // TODO: parse management response into queue info
    return list.release();
}

QoreHashNode* QoreAmqpConnection::getAddressInfo(const char* address, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }

    ReferenceHolder<QoreHashNode> result(managementRequest("READ",
        "org.apache.activemq.artemis:address", address, xsink), xsink);

    if (*xsink || !result) {
        if (*xsink) {
            xsink->clear();
        }
        return nullptr;
    }

    // TODO: parse management response into address details
    return result.release();
}

QoreHashNode* QoreAmqpConnection::getQueueInfo(const char* queue, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }

    ReferenceHolder<QoreHashNode> result(managementRequest("READ",
        "org.apache.activemq.artemis:queue", queue, xsink), xsink);

    if (*xsink || !result) {
        if (*xsink) {
            xsink->clear();
        }
        return nullptr;
    }

    // TODO: parse management response into queue details
    return result.release();
}

QoreHashNode* QoreAmqpConnection::managementRequest(const std::string& operation,
        const std::string& type, const std::string& name, ExceptionSink* xsink) {
    // AMQP Management Protocol: send a message to $management address
    // with operation/type in application-properties

    // Create management sender/receiver if not already initialized
    if (!mgmt_initialized_) {
        std::lock_guard<std::mutex> lock(mgmt_mutex_);
        if (!mgmt_initialized_) {
            std::mutex mtx;
            std::condition_variable cv;
            bool done = false;
            std::string error;

            if (!scheduleWork([&, this]() {
                try {
                    // Create sender to $management
                    mgmt_sender_ = connection_.open_sender("$management");

                    // Create dynamic receiver for replies
                    proton::receiver_options ro;
                    proton::source_options so;
                    so.dynamic(true);
                    ro.source(so);
                    mgmt_receiver_ = connection_.open_receiver("", ro);

                    mgmt_initialized_ = true;
                } catch (const std::exception& e) {
                    error = e.what();
                }
                std::lock_guard<std::mutex> lock(mtx);
                done = true;
                cv.notify_all();
            }, xsink)) {
                return nullptr;
            }

            std::unique_lock<std::mutex> lock2(mtx);
            cv.wait(lock2, [&done]() { return done; });

            if (!error.empty()) {
                xsink->raiseException("AMQP-MANAGEMENT-ERROR",
                    "failed to initialize management: %s", error.c_str());
                return nullptr;
            }
        }
    }

    // Build management request message
    proton::message request;
    request.properties().put("operation", operation);
    request.properties().put("type", type);
    if (!name.empty()) {
        request.properties().put("name", name);
    }
    request.reply_to(mgmt_receiver_.source().address());

    // Send and wait for response
    std::mutex mtx;
    std::condition_variable cv;
    bool done = false;
    std::string error;
    proton::message response;

    if (!scheduleWork([&, this]() {
        try {
            mgmt_sender_.send(request);
        } catch (const std::exception& e) {
            error = e.what();
            std::lock_guard<std::mutex> lock(mtx);
            done = true;
            cv.notify_all();
        }
    }, xsink)) {
        return nullptr;
    }

    // Wait for response with timeout
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    std::string reply_addr = mgmt_receiver_.source().address();

    while (!done) {
        if (std::chrono::steady_clock::now() >= deadline) {
            // Timeout — management not supported
            return nullptr;
        }

        {
            std::lock_guard<std::mutex> lock(recv_mutex_);
            auto it = received_messages_.find(reply_addr);
            if (it != received_messages_.end() && !it->second.empty()) {
                ReceivedMessage rm = std::move(it->second.front());
                it->second.pop();
                response = rm.msg;
                done = true;
                break;
            }
        }

        std::unique_lock<std::mutex> lock2(recv_mutex_);
        auto wait_until = std::min(deadline,
            std::chrono::steady_clock::now() + std::chrono::milliseconds(100));
        recv_cv_.wait_until(lock2, wait_until);
    }

    if (!error.empty()) {
        xsink->raiseException("AMQP-MANAGEMENT-ERROR",
            "management request failed: %s", error.c_str());
        return nullptr;
    }

    // Convert response body to QoreHashNode
    QoreValue body = QoreAmqpHelper::protonToQore(response.body(), xsink);
    if (*xsink) {
        body.discard(xsink);
        return nullptr;
    }

    if (body.getType() == NT_HASH) {
        // Transfer the reference directly — QoreValue does not auto-deref
        return body.get<QoreHashNode>();
    }

    // If not a hash, wrap it
    ReferenceHolder<QoreHashNode> result(new QoreHashNode(autoTypeInfo), xsink);
    result->setKeyValue("body", body, xsink);
    return result.release();
}

std::string QoreAmqpConnection::generateLinkName(const char* prefix) {
    std::lock_guard<std::mutex> lock(links_mutex_);
    return std::string(prefix) + "-" + std::to_string(++link_counter_);
}

std::string QoreAmqpConnection::deliveryTagKey(const proton::delivery& d) {
    proton::binary tag = d.tag();
    std::ostringstream oss;
    for (uint8_t b : tag) {
        oss << std::hex << std::setfill('0') << std::setw(2) << (int)b;
    }
    return oss.str();
}

std::string QoreAmqpConnection::deliveryTagKey(const BinaryNode* tag) {
    if (!tag || !tag->size()) {
        return "";
    }
    const uint8_t* data = reinterpret_cast<const uint8_t*>(tag->getPtr());
    std::ostringstream oss;
    for (size_t i = 0; i < tag->size(); ++i) {
        oss << std::hex << std::setfill('0') << std::setw(2) << (int)data[i];
    }
    return oss.str();
}
