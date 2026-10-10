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
#include <proton/version.h>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <functional>
#include <type_traits>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <memory>
#include <atomic>

//! Struct to hold received message + delivery info
/** The delivery itself stays on the event thread (see QoreAmqpConnection::pending_deliveries_): Qore threads hold
    only its tag
*/
struct ReceivedMessage {
    proton::message msg;
    proton::binary tag;
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

#ifdef DEBUG
    //! Debug builds: blocks the Proton event thread until debugReleaseEventThread() is called
    /** Work scheduled while the event thread is held stays pending, so that tests can cancel a thread waiting for
        work that has not run yet; returns when the event thread is held
    */
    DLLLOCAL void debugHoldEventThread(ExceptionSink* xsink);

    //! Debug builds: waits until the given number of work items have been scheduled since the event thread was held
    DLLLOCAL void debugWaitPendingWork(int64 count, ExceptionSink* xsink);

    //! Debug builds: releases the Proton event thread held by debugHoldEventThread()
    DLLLOCAL void debugReleaseEventThread();
#endif

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
        void on_sender_close(proton::sender& s) override;
        void on_sender_error(proton::sender& s) override;
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

    //! Waits on a condition with cooperative cancellation
    /** With %Qore 3.0 and later, the wait ends as soon as the thread is cancelled or its Program is interrupted;
        with an older %Qore library, cancellation is checked every QORE_IO_POLL_INTERVAL_MS

        @param lock the lock of the condition, locked by the caller; it is locked when the call returns
        @param cv the condition, broadcast under the lock whenever the predicate can change
        @param pred returns true when the wait is over
        @param timeout_ms maximum wait time in milliseconds (-1 for no timeout)
        @param operation description for cancellation messages
        @param xsink exception sink

        @return true if pred became true, false on timeout (no exception) or cancellation (exception raised)
    */
    template <typename Pred>
    DLLLOCAL bool waitWithCancel(std::unique_lock<std::mutex>& lock, QoreCondition& cv, Pred pred,
            int64 timeout_ms, const char* operation, ExceptionSink* xsink) {
        // a QoreCondition waits on the pthread mutex of the lock
        static_assert(std::is_same<std::mutex::native_handle_type, pthread_mutex_t*>::value,
            "std::mutex must wrap a pthread mutex");
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms >= 0 ? timeout_ms : 0);
        while (!pred()) {
            int64 wait_ms = -1;
            if (timeout_ms >= 0) {
                int64 remaining = std::chrono::duration_cast<std::chrono::microseconds>(
                    deadline - std::chrono::steady_clock::now()).count();
                if (remaining <= 0) {
                    return false;
                }
                // rounded up, so that the wait does not end before the deadline
                wait_ms = (remaining + 999) / 1000;
            }
#ifdef _QORE_HAS_CANCELLABLE_POLL
            if (cv.waitWithInterrupt(lock.mutex()->native_handle(), wait_ms, xsink)
                    == QORE_COND_RESULT_INTERRUPTED) {
                return false;
            }
#else
            if (wait_ms < 0 || wait_ms > QORE_IO_POLL_INTERVAL_MS) {
                wait_ms = QORE_IO_POLL_INTERVAL_MS;
            }
            cv.wait2(lock.mutex()->native_handle(), wait_ms);
            if (pred()) {
                return true;
            }
            // the exception is raised without the lock
            lock.unlock();
            bool cancelled = qore_check_cancel(xsink, operation);
            lock.lock();
            if (cancelled) {
                return false;
            }
#endif
        }
        return true;
    }

    //! The state of work that runs on the Proton event thread, shared by the work and the thread waiting for it
    /** The work keeps the state, so it never references the stack of a thread whose wait ended early
        (cancellation, a Program interrupt, or a timeout)
    */
    struct WorkState {
        std::mutex m;
        QoreCondition cond;
        //! set when the work has run, or was skipped because the waiting thread gave up before it ran
        bool done = false;
        //! set when the waiting thread has given up
        bool abandoned = false;
        //! an error raised by the work
        std::string error;
        //! set by the work to release what it created if the waiting thread gives up; called on the event thread
        std::function<void()> release;
    };

    //! Runs work on the Proton event thread and waits until it has run
    /** @param state the state of the work, derived from WorkState
        @param fn the work, called with the state on the event thread unless the waiting thread has given up before;
        it must not reference the stack of the waiting thread
        @param timeout_ms maximum wait time in milliseconds (-1 for no timeout)
        @param operation description for cancellation messages
        @param xsink exception sink

        @return true if the work has run; false if an exception was raised (the work could not be scheduled, or the
        thread was cancelled or its Program interrupted) or the timeout expired (no exception); if the work runs
        after the wait was given up, what it created is released with WorkState::release
    */
    template <typename S, typename F>
    DLLLOCAL bool runWork(const std::shared_ptr<S>& state, F fn, int64 timeout_ms, const char* operation,
            ExceptionSink* xsink) {
        // ends the wait with an error if the work is discarded without running (when the connection is closed)
        struct DiscardGuard {
            std::shared_ptr<S> state;
            bool ran = false;

            ~DiscardGuard() {
                if (ran) {
                    return;
                }
                std::lock_guard<std::mutex> lock(state->m);
                if (!state->done) {
                    state->done = true;
                    state->error = "the connection was closed before the operation ran";
                    state->cond.broadcast();
                }
            }
        };
        std::shared_ptr<DiscardGuard> guard = std::make_shared<DiscardGuard>();
        guard->state = state;
        if (!scheduleWork([guard, fn]() mutable {
            guard->ran = true;
            S& st = *guard->state;
            {
                std::lock_guard<std::mutex> lock(st.m);
                if (st.abandoned) {
                    // the waiting thread gave up before the work ran
                    st.done = true;
                    return;
                }
            }
            try {
                fn(st);
            } catch (const std::exception& e) {
                st.error = e.what();
            }
            bool abandoned;
            {
                std::lock_guard<std::mutex> lock(st.m);
                st.done = true;
                abandoned = st.abandoned;
                st.cond.broadcast();
            }
            if (abandoned && st.release) {
                st.release();
            }
        }, xsink)) {
            return false;
        }

        std::unique_lock<std::mutex> lock(state->m);
        if (waitWithCancel(lock, state->cond, [&state]() { return state->done; }, timeout_ms, operation, xsink)) {
            return true;
        }
        // the wait was given up: what the work creates is released by the work, or here if it has already run
        state->abandoned = true;
        if (!state->done || !state->release) {
            return false;
        }
        lock.unlock();
        scheduleRelease(state);
        return false;
    }

    //! Releases what work created, on the Proton event thread, when the thread that waited for it gives up later
    template <typename S>
    DLLLOCAL void scheduleRelease(const std::shared_ptr<S>& state) {
        if (!state->release) {
            return;
        }
        std::shared_ptr<S> s = state;
        ExceptionSink release_xsink;
        if (!scheduleWork([s]() { s->release(); }, &release_xsink)) {
            // the connection is closed, and its links with it
            release_xsink.clear();
        }
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
            if (!work_queue_->add(std::forward<F>(fn))) {
                xsink->raiseException("AMQP-CONNECTION-ERROR",
                    "connection is closed; cannot schedule work");
                return false;
            }
        } catch (const std::exception& e) {
            xsink->raiseException("AMQP-CONNECTION-ERROR",
                "failed to schedule work: %s", e.what());
            return false;
        }
#ifdef DEBUG
        {
            std::lock_guard<std::mutex> lock(debug_mutex_);
            if (debug_holding_) {
                ++debug_pending_;
                debug_cv_.notify_all();
            }
        }
#endif
        return true;
    }

    //! Closes a sender and forgets it; called on the Proton event thread
    /** @param name the name of the sender
        @param addr the address of the sender, to remove its entry from the reconnection registry
    */
    DLLLOCAL void closeSenderLink(const std::string& name, const std::string& addr);

    //! Closes a receiver and forgets it; called on the Proton event thread
    /** @param name the name of the receiver
        @param addr the address of the receiver, to remove its entry from the reconnection registry
        @param durable true for a durable receiver, whose subscription is kept
    */
    DLLLOCAL void closeReceiverLink(const std::string& name, const std::string& addr, bool durable);

    //! Releases the Proton objects of the connection; called when the event thread has ended
    DLLLOCAL void clearLinks();

    //! Settles a delivery received by receive() with the given disposition
    DLLLOCAL void settle(const BinaryNode* delivery_tag, void (*disposition)(proton::delivery&),
        ExceptionSink* xsink);

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
    QoreCondition connect_cv_;
    std::string connect_error_;

    // Proton objects (links, deliveries) are not thread-safe: they are created, used and destroyed on the event
    // thread only, where they are found by name; Qore threads use the names
    //! the senders by name; event thread only
    std::map<std::string, proton::sender> senders_;
    //! the receivers by name; event thread only
    std::map<std::string, proton::receiver> receivers_;
    //! the names of the open links, protected by links_mutex_
    std::mutex links_mutex_;
    std::set<std::string> sender_names_;
    std::set<std::string> receiver_names_;
    int link_counter_ = 0;

    // Tracks receivers that have been attached at the broker (on_receiver_open
    // has fired).  createReceiver() must wait for this signal before returning,
    // otherwise messages sent by another connection between the local attach
    // and the broker's confirmation can be dropped — with MULTICAST routing
    // (Artemis default) the subscription queue is only created when the link
    // is attached at the broker, so any message that arrives before then has
    // no queue to route to.
    std::mutex attach_mutex_;
    QoreCondition attach_cv_;
    std::set<std::string> attached_receivers_;

    // Received messages queue per receiver
    std::mutex recv_mutex_;
    QoreCondition recv_cv_;
    std::map<std::string, std::queue<ReceivedMessage>> received_messages_;

    // Delivery tracking for accept/reject/release/modify
    //! the deliveries to settle by tag key; event thread only
    std::map<std::string, proton::delivery> pending_deliveries_;
    //! the tag keys of the deliveries to settle, protected by delivery_mutex_
    std::mutex delivery_mutex_;
    std::set<std::string> pending_tags_;

    // Send tracking
    std::mutex send_mutex_;
    QoreCondition send_cv_;
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
    QoreCondition txn_cv_;
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

    //! Schedule a work queue guard to prevent Proton >= 0.40.0 from rejecting
    //! the coordinator link (PROTON-2825 workaround).  Must be called from the
    //! Proton event thread (i.e. inside a work callback).
    DLLLOCAL void scheduleCoordinatorGuard();

    // Management sender/receiver; the links are used on the event thread only
    std::mutex mgmt_mutex_;
    proton::sender mgmt_sender_;
    proton::receiver mgmt_receiver_;
    //! the name of the management reply receiver; set and read on the event thread
    std::string mgmt_receiver_name_;
    std::atomic<bool> mgmt_initialized_{false};
    // Management replies, protected by recv_mutex_ and signaled with recv_cv_
    //! the address that the broker assigned to the dynamic reply receiver, set when it is attached
    std::string mgmt_reply_address_;
    //! the message IDs of the requests waiting for a reply
    std::set<std::string> mgmt_pending_;
    //! replies by the message ID of their request (the correlation ID of the reply)
    std::map<std::string, proton::message> mgmt_replies_;
    //! the counter for management request message IDs
    std::atomic<int64> mgmt_request_counter_{0};

    //! Generate a unique link name
    DLLLOCAL std::string generateLinkName(const char* prefix);

#ifdef DEBUG
    // Debug builds: holding the Proton event thread (see debugHoldEventThread())
    std::mutex debug_mutex_;
    std::condition_variable debug_cv_;
    //! the event thread is held
    bool debug_holding_ = false;
    //! the event thread is to be released
    bool debug_release_ = false;
    //! the number of work items scheduled while the event thread is held
    int64 debug_pending_ = 0;
#endif

    //! Get the delivery tag as a hex string key for the delivery map
    DLLLOCAL static std::string deliveryTagKey(const proton::delivery& d);
    DLLLOCAL static std::string deliveryTagKey(const BinaryNode* tag);
};

#endif // _QORE_AMQP_CONNECTION_H
