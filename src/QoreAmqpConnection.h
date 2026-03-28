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

#include <proton/connection.h>
#include <proton/link.h>
#include <proton/session.h>
#include <proton/delivery.h>
#include <proton/disposition.h>
#include <proton/terminus.h>
#include <proton/codec.h>

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

    //! Send a batch of messages
    DLLLOCAL QoreListNode* sendBatch(const char* sender_name, const QoreListNode* msgs,
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

    //! Close a sender link
    DLLLOCAL void closeSender(const char* sender_name, ExceptionSink* xsink);

    //! Close a receiver link
    DLLLOCAL void closeReceiver(const char* receiver_name, ExceptionSink* xsink);

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

    //! Get connection statistics
    DLLLOCAL QoreHashNode* getStatistics(ExceptionSink* xsink);

    //! Get and drain pending connection events
    DLLLOCAL QoreListNode* getConnectionEvents(ExceptionSink* xsink);

    //! Set whether links should be automatically recovered on reconnect
    DLLLOCAL void setAutoRecoverLinks(bool recover);

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

    //! Check network security access for the configured URL
    /** Checks with QoreNetworkSecurityManager before allowing a connection.
        @return true if access is allowed, false if denied (exception raised)
    */
    DLLLOCAL bool checkNetworkAccess(ExceptionSink* xsink) const;

    //! Wait on a condition variable with cooperative cancellation
    /** Polls every QORE_IO_POLL_INTERVAL_MS, checking qore_check_cancel().
        @param mtx the mutex (must be locked by caller via unique_lock)
        @param cv the condition variable
        @param pred predicate that returns true when done
        @param timeout_ms maximum wait time (-1 for no timeout)
        @param operation description for cancellation messages
        @param xsink exception sink
        @return true if pred became true, false on timeout or cancellation
    */
    template <typename Pred>
    DLLLOCAL bool waitWithCancel(std::unique_lock<std::mutex>& lock,
            std::condition_variable& cv, Pred pred, int timeout_ms,
            const char* operation, ExceptionSink* xsink) {
        auto deadline = timeout_ms >= 0
            ? std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms)
            : std::chrono::steady_clock::time_point::max();

        while (!pred()) {
            auto wait_until = std::min(deadline,
                std::chrono::steady_clock::now()
                    + std::chrono::milliseconds(QORE_IO_POLL_INTERVAL_MS));

            cv.wait_until(lock, wait_until);

            if (pred()) {
                return true;
            }

            // Check timeout
            if (timeout_ms >= 0 && std::chrono::steady_clock::now() >= deadline) {
                return false;
            }

            // Check cooperative cancellation (unlock to avoid holding lock during xsink ops)
            lock.unlock();
            if (qore_check_cancel(xsink, operation)) {
                lock.lock();
                return false;
            }
            lock.lock();
        }
        return true;
    }

    //! Safely schedule work on the proton work queue; returns false and raises
    //! an exception if the connection is closed or the work queue is unavailable.
    template <typename F>
    DLLLOCAL bool scheduleWork(F&& fn, ExceptionSink* xsink) {
        std::lock_guard<std::mutex> lock(wq_mutex_);
        if (!work_queue_) {
            xsink->raiseException("AMQP-CONNECTION-ERROR",
                "connection is closed; cannot schedule work");
            return false;
        }
        try {
            work_queue_->add(std::forward<F>(fn));
        } catch (const std::exception& e) {
            xsink->raiseException("AMQP-CONNECTION-ERROR",
                "failed to schedule work: %s", e.what());
            return false;
        }
        return true;
    }

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

    // Connection state (work_queue_ protected by wq_mutex_)
    proton::connection connection_;
    mutable std::mutex wq_mutex_;
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

    // Connection event queue (thread-safe, pushed from Proton event thread)
    struct ConnectionEvent {
        std::string event_id;
        std::string error;
        int64 timestamp_us;  // microseconds since epoch
    };
    std::mutex event_mutex_;
    std::queue<ConnectionEvent> event_queue_;
    static constexpr size_t MAX_EVENT_QUEUE = 100;

    //! Push a connection event (called from Proton event thread)
    DLLLOCAL void pushEvent(const std::string& event_id, const std::string& error = "");

    // Cached C-level pointers for coordinator link creation
    // Set from on_sender_open/on_receiver_open where we have safe C access
    pn_session_t* cached_session_ = nullptr;

    // Link registry for reconnection recovery
    struct LinkInfo {
        std::string address;
        bool is_sender;
        bool is_durable = false;
        std::string subscription_name;
        // Store serializable options for re-creation
    };
    std::mutex registry_mutex_;
    std::vector<LinkInfo> link_registry_;
    bool was_connected_ = false;  // true if we were ever connected (for reconnect detection)
    bool auto_recover_links_ = true;

    // Connection statistics
    std::atomic<int64> messages_sent_{0};
    std::atomic<int64> messages_received_{0};
    std::atomic<int64> bytes_sent_{0};
    std::atomic<int64> bytes_received_{0};
    std::atomic<int64> errors_{0};
    int64 connected_since_epoch_us_ = 0;  // microseconds since epoch, 0 if not connected

    // Transaction state
    std::mutex txn_mutex_;
    std::condition_variable txn_cv_;
    std::atomic<bool> txn_active_{false};
    pn_link_t* txn_coordinator_link_ = nullptr;  // C-level coordinator sender
    proton::binary txn_id_;
    bool txn_coordinator_ready_ = false;  // coordinator has credit
    bool txn_declare_done_ = false;       // declare response received
    bool txn_discharge_done_ = false;     // discharge response received
    std::string txn_error_;

    //! The link name used for the transaction coordinator
    static constexpr const char* TXN_COORDINATOR_NAME = "txn-coordinator";

    //! Send a Discharge message on the coordinator and wait for confirmation
    DLLLOCAL void discharge(bool fail, ExceptionSink* xsink);

    // Management sender/receiver
    std::mutex mgmt_mutex_;
    proton::sender mgmt_sender_;
    proton::receiver mgmt_receiver_;
    std::atomic<bool> mgmt_initialized_{false};

    //! Generate a unique link name
    DLLLOCAL std::string generateLinkName(const char* prefix);

    //! Get the delivery tag as a hex string key for the delivery map
    DLLLOCAL static std::string deliveryTagKey(const proton::delivery& d);
    DLLLOCAL static std::string deliveryTagKey(const BinaryNode* tag);
};

#endif // _QORE_AMQP_CONNECTION_H
