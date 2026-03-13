/* -*- mode: c++; indent-tabs-mode: nil -*- */
/** @file QoreAmqpConnection.h QoreAmqpConnection class definition */
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

#ifndef _QORE_AMQP_CONNECTION_H
#define _QORE_AMQP_CONNECTION_H

#include "amqp-module.h"
#include "QoreAmqpHelper.h"
#include "QoreAmqpMessage.h"

#include <proton/container.hpp>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/session.hpp>
#include <proton/sender.hpp>
#include <proton/receiver.hpp>
#include <proton/delivery.hpp>
#include <proton/tracker.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/work_queue.hpp>
#include <proton/source_options.hpp>
#include <proton/target_options.hpp>
#include <proton/receiver_options.hpp>
#include <proton/sender_options.hpp>
#include <proton/ssl.hpp>
#include <proton/sasl.hpp>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <map>
#include <queue>
#include <string>
#include <memory>
#include <atomic>

//! Struct to hold received message + delivery info
struct ReceivedMessage {
    proton::message msg;
    proton::delivery delivery;
};

//! Wraps a proton::container + proton::messaging_handler for AMQP 1.0 connections
class QoreAmqpConnection : public AbstractPrivateData {
public:
    //! Constructor: create from connection options hash
    DLLLOCAL QoreAmqpConnection(const QoreHashNode* options, ExceptionSink* xsink);

    DLLLOCAL ~QoreAmqpConnection() override;

    //! Connect to the broker
    DLLLOCAL void connect(ExceptionSink* xsink);

    //! Close the connection
    DLLLOCAL void close(ExceptionSink* xsink);

    //! Check if connected
    DLLLOCAL bool isConnected() const;

    //! Create a sender link
    DLLLOCAL QoreStringNode* createSender(const char* address, const QoreHashNode* opts,
        ExceptionSink* xsink);

    //! Create a receiver link
    DLLLOCAL QoreStringNode* createReceiver(const char* address, const QoreHashNode* opts,
        const QoreHashNode* filter, ExceptionSink* xsink);

    //! Send a message
    DLLLOCAL QoreHashNode* send(const char* sender_name, const QoreAmqpMessage& msg,
        const QoreHashNode* opts, ExceptionSink* xsink);

    //! Receive a message (blocks with cooperative cancellation)
    DLLLOCAL QoreObject* receive(QoreObject* self, const char* receiver_name,
        int64 timeout_ms, ExceptionSink* xsink);

    //! Accept a delivery
    DLLLOCAL void accept(const BinaryNode* delivery_tag, ExceptionSink* xsink);

    //! Reject a delivery
    DLLLOCAL void reject(const BinaryNode* delivery_tag, ExceptionSink* xsink);

    //! Release a delivery
    DLLLOCAL void release(const BinaryNode* delivery_tag, ExceptionSink* xsink);

    //! Modify a delivery
    DLLLOCAL void modify(const BinaryNode* delivery_tag, bool failed, bool undeliverable,
        const QoreHashNode* annotations, ExceptionSink* xsink);

    //! Begin a transaction
    DLLLOCAL void beginTransaction(ExceptionSink* xsink);

    //! Commit the current transaction
    DLLLOCAL void commitTransaction(ExceptionSink* xsink);

    //! Rollback the current transaction
    DLLLOCAL void rollbackTransaction(ExceptionSink* xsink);

    //! Check if a transaction is active
    DLLLOCAL bool inTransaction() const;

    //! Create a durable receiver
    DLLLOCAL QoreStringNode* createDurableReceiver(const char* address,
        const char* subscription_name, const QoreHashNode* opts,
        const QoreHashNode* filter, ExceptionSink* xsink);

    //! Close a durable receiver without unsubscribing
    DLLLOCAL void closeDurableReceiver(const char* receiver_name, ExceptionSink* xsink);

    //! Unsubscribe a durable subscription
    DLLLOCAL void unsubscribeDurable(const char* subscription_name, ExceptionSink* xsink);

    //! Query addresses via AMQP Management Protocol
    DLLLOCAL QoreListNode* queryAddresses(ExceptionSink* xsink);

    //! Query queues via AMQP Management Protocol
    DLLLOCAL QoreListNode* queryQueues(const char* address, ExceptionSink* xsink);

    //! Get info about a specific address
    DLLLOCAL QoreHashNode* getAddressInfo(const char* address, ExceptionSink* xsink);

    //! Get info about a specific queue
    DLLLOCAL QoreHashNode* getQueueInfo(const char* queue, ExceptionSink* xsink);

private:
    //! The messaging handler that receives proton events
    class Handler : public proton::messaging_handler {
    public:
        Handler(QoreAmqpConnection& conn) : conn_(conn) {}

        void on_container_start(proton::container& c) override;
        void on_connection_open(proton::connection& c) override;
        void on_connection_close(proton::connection& c) override;
        void on_connection_error(proton::connection& c) override;
        void on_sender_open(proton::sender& s) override;
        void on_receiver_open(proton::receiver& r) override;
        void on_sendable(proton::sender& s) override;
        void on_message(proton::delivery& d, proton::message& m) override;
        void on_tracker_accept(proton::tracker& t) override;
        void on_tracker_reject(proton::tracker& t) override;
        void on_tracker_settle(proton::tracker& t) override;
        void on_transport_error(proton::transport& t) override;
        void on_error(const proton::error_condition& ec) override;

    private:
        QoreAmqpConnection& conn_;
    };

    //! Ensure the connection is open, raising an exception if not
    DLLLOCAL bool checkConnected(ExceptionSink* xsink) const;

    //! Send an AMQP management request and receive a response
    DLLLOCAL QoreHashNode* managementRequest(const std::string& operation,
        const std::string& type, const std::string& name, ExceptionSink* xsink);

    // Connection options
    std::string url_;
    std::string container_id_;
    std::string virtual_host_;
    int heartbeat_ = 0;
    int idle_timeout_ = 0;
    int max_frame_size_ = 0;
    bool reconnect_ = false;
    int max_reconnect_attempts_ = 0;
    int reconnect_delay_ms_ = 1000;

    // SSL options
    std::string ssl_ca_cert_;
    std::string ssl_client_cert_;
    std::string ssl_client_key_;
    bool ssl_verify_ = true;

    // SASL options
    std::string sasl_mechanism_;
    std::string sasl_username_;
    std::string sasl_password_;

    // Proton objects
    Handler handler_;
    std::unique_ptr<proton::container> container_;
    std::thread container_thread_;

    // Connection state
    proton::connection connection_;
    proton::work_queue* work_queue_ = nullptr;
    std::atomic<bool> connected_{false};
    std::atomic<bool> closing_{false};

    // Synchronization for connection establishment
    std::mutex connect_mutex_;
    std::condition_variable connect_cv_;
    std::string connect_error_;

    // Sender/receiver maps
    std::mutex links_mutex_;
    std::map<std::string, proton::sender> senders_;
    std::map<std::string, proton::receiver> receivers_;
    int link_counter_ = 0;

    // Received messages queue per receiver
    std::mutex recv_mutex_;
    std::condition_variable recv_cv_;
    std::map<std::string, std::queue<ReceivedMessage>> received_messages_;

    // Delivery tracking for accept/reject/release/modify
    std::mutex delivery_mutex_;
    std::map<std::string, proton::delivery> pending_deliveries_;

    // Send tracking
    std::mutex send_mutex_;
    std::condition_variable send_cv_;
    struct SendResult {
        bool done = false;
        bool accepted = false;
        std::string error;
        proton::binary tag;
    };
    std::map<std::string, SendResult> send_results_;

    // Transaction state
    std::mutex txn_mutex_;
    bool txn_active_ = false;

    // Management sender/receiver
    std::mutex mgmt_mutex_;
    proton::sender mgmt_sender_;
    proton::receiver mgmt_receiver_;
    bool mgmt_initialized_ = false;

    //! Generate a unique link name
    DLLLOCAL std::string generateLinkName(const char* prefix);

    //! Get the delivery tag as a hex string key for the delivery map
    DLLLOCAL static std::string deliveryTagKey(const proton::delivery& d);
    DLLLOCAL static std::string deliveryTagKey(const BinaryNode* tag);
};

#endif // _QORE_AMQP_CONNECTION_H
