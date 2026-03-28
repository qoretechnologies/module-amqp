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

#include <proton/message.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <sstream>
#include <iomanip>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

// --- Proton C/C++ interop helper ---
// Proton C++ types store the underlying C pointer as their first (or only) data member.
// Polymorphic types (sender, receiver, connection) have a vtable pointer before the
// data, so the C pointer is at offset sizeof(void*). Non-polymorphic types (tracker,
// transfer) store the C pointer at offset 0.
template <typename CType, typename CppType>
CType* proton_unwrap(const CppType& obj) {
    CType* ptr;
    constexpr size_t offset = std::is_polymorphic_v<CppType> ? sizeof(void*) : 0;
    std::memcpy(&ptr, reinterpret_cast<const char*>(&obj) + offset, sizeof(ptr));
    return ptr;
}

// --- Helpers for management response parsing ---

namespace {

// Safely get a string from a Qore hash, empty if missing
std::string getStringVal(const QoreHashNode* h, const char* key) {
    QoreValue v = h->getKeyValue(key);
    if (v.isNullOrNothing()) {
        return {};
    }
    if (v.getType() == NT_STRING) {
        return v.get<const QoreStringNode>()->c_str();
    }
    QoreStringValueHelper str(v);
    return str->c_str();
}

// Safely get an int from a Qore hash, default if missing
int64 getIntVal(const QoreHashNode* h, const char* key, int64 def = 0) {
    QoreValue v = h->getKeyValue(key);
    if (v.isNullOrNothing()) {
        return def;
    }
    return v.getAsBigInt();
}

// Try multiple key names, return first found int
int64 getIntMulti(const QoreHashNode* h, std::initializer_list<const char*> keys, int64 def = 0) {
    for (const char* key : keys) {
        QoreValue v = h->getKeyValue(key);
        if (!v.isNullOrNothing()) {
            return v.getAsBigInt();
        }
    }
    return def;
}

// Safely get a bool from a Qore hash
bool getBoolVal(const QoreHashNode* h, const char* key, bool def = false) {
    QoreValue v = h->getKeyValue(key);
    if (v.isNullOrNothing()) {
        return def;
    }
    return v.getAsBool();
}

// Extract routing type string from a hash (handles both "routingType" and "routingTypes" list)
std::string getRoutingType(const QoreHashNode* h) {
    std::string rt = getStringVal(h, "routingType");
    if (rt.empty()) {
        QoreValue rt_val = h->getKeyValue("routingTypes");
        if (!rt_val.isNullOrNothing() && rt_val.getType() == NT_LIST) {
            const QoreListNode* rt_list = rt_val.get<const QoreListNode>();
            if (rt_list->size() > 0) {
                QoreValue first = rt_list->retrieveEntry(0);
                if (first.getType() == NT_STRING) {
                    rt = first.get<const QoreStringNode>()->c_str();
                }
            }
        }
    }
    std::transform(rt.begin(), rt.end(), rt.begin(), ::tolower);
    if (rt.empty()) {
        rt = "anycast";
    }
    return rt;
}

// Build an AmqpAddressInfo hash from a raw attribute map
QoreHashNode* buildAddressInfo(const QoreHashNode* entity, ExceptionSink* xsink) {
    ReferenceHolder<QoreHashNode> info(new QoreHashNode(hashdeclAmqpAddressInfo, xsink), xsink);
    info->setKeyValue("name", new QoreStringNode(getStringVal(entity, "name")), xsink);
    info->setKeyValue("routing_type", new QoreStringNode(getRoutingType(entity)), xsink);
    info->setKeyValue("queue_count", getIntMulti(entity, {"queueCount", "queue_count"}), xsink);
    info->setKeyValue("message_count", getIntMulti(entity, {"messageCount", "message_count", "messages"}), xsink);
    if (*xsink) {
        return nullptr;
    }
    return info.release();
}

// Build an AmqpQueueInfo hash from a raw attribute map
QoreHashNode* buildQueueInfo(const QoreHashNode* entity, ExceptionSink* xsink) {
    ReferenceHolder<QoreHashNode> info(new QoreHashNode(hashdeclAmqpQueueInfo, xsink), xsink);
    info->setKeyValue("name", new QoreStringNode(getStringVal(entity, "name")), xsink);
    info->setKeyValue("address", new QoreStringNode(getStringVal(entity, "address")), xsink);
    info->setKeyValue("routing_type", new QoreStringNode(getRoutingType(entity)), xsink);
    info->setKeyValue("durable", getBoolVal(entity, "durable"), xsink);
    info->setKeyValue("message_count", getIntMulti(entity, {"messageCount", "message_count", "messages"}), xsink);
    info->setKeyValue("consumer_count", getIntMulti(entity, {"consumerCount", "consumer_count", "consumers"}), xsink);

    std::string filter = getStringVal(entity, "filterString");
    if (!filter.empty()) {
        info->setKeyValue("filter", new QoreStringNode(filter), xsink);
    }
    if (*xsink) {
        return nullptr;
    }
    return info.release();
}

// Parse a management QUERY response body — supports row format (list of maps)
// and raw hash (single entity). Returns entities found.
template <typename BuildFn>
QoreListNode* parseQueryResponse(const QoreHashNode* result, const TypedHashDecl* decl,
        BuildFn build_fn, ExceptionSink* xsink) {
    ReferenceHolder<QoreListNode> list(new QoreListNode(decl->getTypeInfo()), xsink);

    // Check for "body" key wrapping a list (row format)
    QoreValue body_val = result->getKeyValue("body");
    if (!body_val.isNullOrNothing() && body_val.getType() == NT_LIST) {
        const QoreListNode* body_list = body_val.get<const QoreListNode>();
        for (size_t i = 0; i < body_list->size(); ++i) {
            QoreValue elem = body_list->retrieveEntry(i);
            if (elem.getType() != NT_HASH) {
                continue;
            }
            QoreHashNode* info = build_fn(elem.get<const QoreHashNode>(), xsink);
            if (*xsink) {
                return nullptr;
            }
            if (info) {
                list->push(info, xsink);
            }
        }
        return list.release();
    }

    // Check if the result itself is a single entity (has a "name" key)
    QoreValue name_val = result->getKeyValue("name");
    if (!name_val.isNullOrNothing() && name_val.getType() == NT_STRING) {
        QoreHashNode* info = build_fn(result, xsink);
        if (*xsink) {
            return nullptr;
        }
        if (info) {
            list->push(info, xsink);
        }
    }
    // else: unrecognized format, return empty list

    return list.release();
}

} // anonymous namespace

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
    bool is_reconnect = conn_.was_connected_;
    {
        std::lock_guard<std::mutex> lock(conn_.wq_mutex_);
        conn_.connection_ = c;
        conn_.work_queue_ = &c.work_queue();
    }
    conn_.connected_ = true;
    conn_.was_connected_ = true;
    auto now = std::chrono::system_clock::now();
    conn_.connected_since_epoch_us_ = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();

    if (is_reconnect) {
        conn_.pushEvent("amqp-reconnected");

        // Recover links if auto_recover_links_ is enabled
        if (conn_.auto_recover_links_) {
            std::lock_guard<std::mutex> reg_lock(conn_.registry_mutex_);
            for (const auto& info : conn_.link_registry_) {
                try {
                    if (info.is_sender) {
                        proton::sender s = conn_.connection_.open_sender(info.address);
                        std::lock_guard<std::mutex> lock(conn_.links_mutex_);
                        conn_.senders_[s.name()] = s;
                    } else if (info.is_durable) {
                        proton::receiver_options ro;
                        proton::source_options so;
                        so.address(info.address);
                        so.durability_mode(proton::source::UNSETTLED_STATE);
                        so.expiry_policy(proton::source::NEVER);
                        ro.source(so);
                        ro.auto_accept(false);
                        ro.name(info.subscription_name);
                        proton::receiver r = conn_.connection_.open_receiver(info.address, ro);
                        std::lock_guard<std::mutex> lock(conn_.links_mutex_);
                        conn_.receivers_[r.name()] = r;
                    } else {
                        proton::receiver_options ro;
                        ro.auto_accept(false);
                        proton::receiver r = conn_.connection_.open_receiver(info.address, ro);
                        std::lock_guard<std::mutex> lock(conn_.links_mutex_);
                        conn_.receivers_[r.name()] = r;
                    }
                } catch (...) {
                    // Best-effort recovery — don't fail the reconnect
                }
            }
        }
    } else {
        conn_.pushEvent("amqp-connected");
    }

    std::lock_guard<std::mutex> lock(conn_.connect_mutex_);
    conn_.connect_error_.clear();
    conn_.connect_cv_.notify_all();
}

void QoreAmqpConnection::Handler::on_connection_close(proton::connection& c) {
    conn_.connected_ = false;
    conn_.connected_since_epoch_us_ = 0;
    conn_.pushEvent("amqp-disconnected");
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
    conn_.pushEvent("amqp-error", err);

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
    // Check if this is the transaction coordinator (compare C link pointers)
    pn_link_t* link = proton_unwrap<pn_link_t>(s);
    if (link == conn_.txn_coordinator_link_) {
        std::lock_guard<std::mutex> lock(conn_.txn_mutex_);
        conn_.txn_coordinator_ready_ = true;
        conn_.txn_cv_.notify_all();
    }
}

void QoreAmqpConnection::Handler::on_message(proton::delivery& d, proton::message& m) {
    std::string receiver_name = d.receiver().name();
    ++conn_.messages_received_;

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
        oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(b);
    }
    std::string tag_key = oss.str();
    {
        std::lock_guard<std::mutex> lock(conn_.send_mutex_);
        auto it = conn_.send_results_.find(tag_key);
        if (it != conn_.send_results_.end()) {
            it->second.done = true;
            it->second.accepted = true;
            ++conn_.messages_sent_;
        }
        conn_.send_cv_.notify_all();
    }
}

void QoreAmqpConnection::Handler::on_tracker_reject(proton::tracker& t) {
    proton::binary tag = t.tag();
    std::ostringstream oss;
    for (uint8_t b : tag) {
        oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(b);
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
    // Check if this is a transaction coordinator response (by link pointer)
    proton::sender tracker_sender = t.sender();
    pn_link_t* tracker_link = proton_unwrap<pn_link_t>(tracker_sender);
    if (tracker_link == conn_.txn_coordinator_link_) {
        // Get the pn_delivery_t* directly from the tracker
        pn_delivery_t* dlv = proton_unwrap<pn_delivery_t>(t);
        uint64_t state = pn_delivery_remote_state(dlv);

        if (state == 0x33) {
            // Declared (0x33): extract txn-id from remote disposition data
            pn_disposition_t* disp = pn_delivery_remote(dlv);
            pn_data_t* data = pn_disposition_data(disp);
            bool extracted = false;
            if (data) {
                pn_data_rewind(data);
                // Walk through the data to find a binary value (the txn-id)
                // The format may be: raw binary, or inside a list, or described
                while (pn_data_next(data)) {
                    pn_type_t type = pn_data_type(data);
                    if (type == PN_BINARY) {
                        pn_bytes_t bytes = pn_data_get_binary(data);
                        if (bytes.size > 0) {
                            std::lock_guard<std::mutex> lock(conn_.txn_mutex_);
                            conn_.txn_id_.assign(bytes.start, bytes.start + bytes.size);
                            conn_.txn_declare_done_ = true;
                            conn_.txn_cv_.notify_all();
                            extracted = true;
                            break;
                        }
                    } else if (type == PN_LIST || type == PN_DESCRIBED) {
                        pn_data_enter(data);
                        continue;
                    }
                }
            }
            if (!extracted) {
                // Try getting the delivery tag as fallback txn-id
                pn_delivery_tag_t tag = pn_delivery_tag(dlv);
                if (tag.size > 0) {
                    std::lock_guard<std::mutex> lock(conn_.txn_mutex_);
                    conn_.txn_id_.assign(tag.start, tag.start + tag.size);
                    conn_.txn_declare_done_ = true;
                    conn_.txn_cv_.notify_all();
                    extracted = true;
                }
            }
            if (!extracted) {
                std::lock_guard<std::mutex> lock(conn_.txn_mutex_);
                conn_.txn_error_ = "failed to extract transaction ID from Declared response";
                conn_.txn_declare_done_ = true;
                conn_.txn_cv_.notify_all();
            }
        } else if (state == PN_ACCEPTED) {
            // Discharge accepted (commit/rollback succeeded)
            std::lock_guard<std::mutex> lock(conn_.txn_mutex_);
            conn_.txn_discharge_done_ = true;
            conn_.txn_cv_.notify_all();
        } else if (state == PN_REJECTED) {
            std::lock_guard<std::mutex> lock(conn_.txn_mutex_);
            conn_.txn_error_ = "transaction discharge rejected by broker";
            conn_.txn_discharge_done_ = true;
            conn_.txn_cv_.notify_all();
        } else {
            std::lock_guard<std::mutex> lock(conn_.txn_mutex_);
            std::ostringstream oss;
            oss << "unexpected coordinator response state: 0x"
                << std::hex << state;
            conn_.txn_error_ = oss.str();
            conn_.txn_declare_done_ = true;
            conn_.txn_discharge_done_ = true;
            conn_.txn_cv_.notify_all();
        }
        return;
    }
}

void QoreAmqpConnection::Handler::on_transport_error(proton::transport& t) {
    std::string err = t.error().what();
    conn_.connected_ = false;
    conn_.pushEvent("amqp-error", err);

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
    ++conn_.errors_;

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
        heartbeat_ = static_cast<int>(v.getAsBigInt());
    }

    v = options->getKeyValue("idle_timeout");
    if (!v.isNullOrNothing()) {
        idle_timeout_ = static_cast<int>(v.getAsBigInt());
    }

    v = options->getKeyValue("max_frame_size");
    if (!v.isNullOrNothing()) {
        max_frame_size_ = static_cast<int>(v.getAsBigInt());
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
        max_reconnect_attempts_ = static_cast<int>(v.getAsBigInt());
    }

    v = options->getKeyValue("reconnect_delay_ms");
    if (!v.isNullOrNothing()) {
        reconnect_delay_ms_ = static_cast<int>(v.getAsBigInt());
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

    // Note: Proton extracts credentials from the URL automatically when
    // amqp://user:pass@host is passed to container::connect(). We only need
    // to extract them here for the checkNetworkAccess() host parsing.
    // Explicit SASL options (set above) override URL credentials in Proton.
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

    // Check cooperative cancellation before network I/O
    if (qore_check_cancel(xsink, "AmqpConnection::connect")) {
        return;
    }

    // Check network security access
    if (!checkNetworkAccess(xsink)) {
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

    // Wait for connection with cooperative cancellation (30s timeout)
    bool completed = waitWithCancel(lock, connect_cv_, [this]() {
        return connected_.load() || !connect_error_.empty();
    }, 30000, "AmqpConnection::connect", xsink);

    if (*xsink) {
        // Cancelled — clean up
        lock.unlock();
        if (container_thread_.joinable()) {
            try {
                container_->stop();
            } catch (...) {}
            container_thread_.join();
        }
        container_.reset();
        return;
    }

    if (!completed) {
        lock.unlock();
        xsink->raiseException("AMQP-CONNECTION-ERROR",
            "connection timed out after 30 seconds");
        if (container_thread_.joinable()) {
            try {
                container_->stop();
            } catch (...) {}
            container_thread_.join();
        }
        container_.reset();
        return;
    }

    if (!connect_error_.empty()) {
        std::string err = connect_error_;
        lock.unlock();
        xsink->raiseException("AMQP-CONNECTION-ERROR", "failed to connect: %s",
            err.c_str());
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

bool QoreAmqpConnection::checkNetworkAccess(ExceptionSink* xsink) const {
    QoreSandboxManagerHelper smh;
    if (!smh) {
        return true;
    }

    std::string host;
    int port;
    QoreAmqpHelper::parseUrlHostPort(url_, host, port);

    // Phase 1: preliminary hostname pattern check (pre-DNS)
    const QoreNetworkSecurityManager& net = smh->network();
    if (!net.checkHostname(host.c_str(), port, QSEC_NET_TCP)) {
        xsink->raiseException("NETWORK-ACCESS-DENIED",
            "access to '%s:%d' is denied by network security policy",
            host.c_str(), port);
        return false;
    }

    // Phase 2: resolve DNS and check all resolved IPs against CIDR deny lists
    // This prevents SSRF where a hostname resolves to a private/internal IP
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    std::string port_str = std::to_string(port);
    struct addrinfo* result = nullptr;
    int rc = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result);
    if (rc != 0) {
        xsink->raiseException("AMQP-CONNECTION-ERROR",
            "failed to resolve hostname '%s': %s", host.c_str(), gai_strerror(rc));
        return false;
    }

    // Check every resolved address; deny if any fails the security check
    for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        if (!smh->checkNetworkAccess(rp->ai_addr, rp->ai_addrlen,
                QSEC_NET_TCP, xsink)) {
            freeaddrinfo(result);
            return false;
        }
    }

    freeaddrinfo(result);
    return true;
}

QoreStringNode* QoreAmqpConnection::createSender(const char* address, const QoreHashNode* opts,
        ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }
    if (qore_check_cancel(xsink, "AmqpConnection::createSender")) {
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
            name = s.name();
            {
                std::lock_guard<std::mutex> lock(links_mutex_);
                senders_[name] = s;
            }
            // Register for reconnect recovery
            {
                std::lock_guard<std::mutex> lock(registry_mutex_);
                link_registry_.push_back(LinkInfo{addr, true, false, ""});
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
    if (!waitWithCancel(lock, cv, [&done]() { return done; },
            30000, "AmqpConnection::createSender", xsink)) {
        if (!*xsink) {
            xsink->raiseException("AMQP-SENDER-ERROR",
                "timed out creating sender for '%s'", address);
        }
        return nullptr;
    }

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
    if (qore_check_cancel(xsink, "AmqpConnection::createReceiver")) {
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
            credit = static_cast<int>(v.getAsBigInt());
        }
        v = opts->getKeyValue("auto_accept");
        if (!v.isNullOrNothing()) {
            auto_accept = v.getAsBool();
        }
        v = opts->getKeyValue("prefetch");
        if (!v.isNullOrNothing()) {
            prefetch = static_cast<int>(v.getAsBigInt());
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
            name = r.name();
            {
                std::lock_guard<std::mutex> lock(links_mutex_);
                receivers_[name] = r;
            }
            // Register for reconnect recovery
            {
                std::lock_guard<std::mutex> lock(registry_mutex_);
                link_registry_.push_back(LinkInfo{addr, false, false, ""});
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
    if (!waitWithCancel(lock, cv, [&done]() { return done; },
            30000, "AmqpConnection::createReceiver", xsink)) {
        if (!*xsink) {
            xsink->raiseException("AMQP-RECEIVER-ERROR",
                "timed out creating receiver for '%s'", address);
        }
        return nullptr;
    }

    if (!error.empty()) {
        xsink->raiseException("AMQP-RECEIVER-ERROR", "failed to create receiver for '%s': %s",
            address, error.c_str());
        return nullptr;
    }

    return new QoreStringNode(name);
}

void QoreAmqpConnection::closeSender(const char* sender_name, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    std::string name(sender_name);
    proton::sender sender;

    {
        std::lock_guard<std::mutex> lock(links_mutex_);
        auto it = senders_.find(name);
        if (it == senders_.end()) {
            xsink->raiseException("AMQP-SENDER-ERROR", "sender '%s' not found", sender_name);
            return;
        }
        sender = it->second;
        senders_.erase(it);
    }

    // Remove from link registry
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        link_registry_.erase(
            std::remove_if(link_registry_.begin(), link_registry_.end(),
                [&sender](const LinkInfo& li) {
                    return li.is_sender && li.address == sender.target().address();
                }),
            link_registry_.end());
    }

    scheduleWork([sender]() mutable {
        sender.close();
    }, xsink);
}

void QoreAmqpConnection::closeReceiver(const char* receiver_name, ExceptionSink* xsink) {
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

    // Clean up any pending messages for this receiver
    {
        std::lock_guard<std::mutex> lock(recv_mutex_);
        received_messages_.erase(name);
    }

    scheduleWork([receiver]() mutable {
        receiver.close();
    }, xsink);
}

QoreHashNode* QoreAmqpConnection::send(const char* sender_name, const QoreAmqpMessage& msg,
        const QoreHashNode* opts, ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return nullptr;
    }
    if (qore_check_cancel(xsink, "AmqpConnection::send")) {
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
            pmsg.ttl(proton::duration(static_cast<int64_t>(v.getAsBigInt())));
        }
        v = opts->getKeyValue("priority");
        if (!v.isNullOrNothing()) {
            pmsg.priority(static_cast<uint8_t>(v.getAsBigInt()));
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
            pmsg.delivery_count(static_cast<uint32_t>(v.getAsBigInt()));
        }
    }

    // Send via work queue and wait for broker confirmation
    std::string tag_key;
    std::mutex mtx;
    std::condition_variable cv;
    bool send_done = false;
    bool sent = false;
    std::string error;

    // Capture transaction state for the lambda
    bool is_transacted = txn_active_.load();
    proton::binary send_txn_id;
    if (is_transacted) {
        std::lock_guard<std::mutex> lock(txn_mutex_);
        send_txn_id = txn_id_;
    }

    if (!scheduleWork([&, this, is_transacted, send_txn_id]() {
        try {
            // Use Proton C API for full control over the delivery — this enables
            // setting TransactionalState on the TRANSFER frame for transacted sends
            pn_link_t* c_link = proton_unwrap<pn_link_t>(sender);
            pn_message_t* c_msg = proton_unwrap<pn_message_t>(pmsg);

            // Generate a unique delivery tag
            static std::atomic<int> send_tag_counter{0};
            std::string dtag = "s-" + std::to_string(++send_tag_counter);

            // Create delivery with explicit tag
            pn_delivery_t* dlv = pn_delivery(c_link,
                pn_dtag(dtag.c_str(), dtag.size()));

            // If transacted, set TransactionalState on the delivery BEFORE
            // sending. The Proton transport includes delivery->local state in
            // the TRANSFER frame when it's set before the link bytes are sent.
            if (is_transacted && !send_txn_id.empty()) {
                pn_delivery_update(dlv, 0x34);  // TransactionalState descriptor
                pn_data_t* disp_data = pn_disposition_data(
                    pn_delivery_local(dlv));
                pn_data_clear(disp_data);
                // TransactionalState fields: list(binary txn-id, *outcome)
                pn_data_put_list(disp_data);
                pn_data_enter(disp_data);
                pn_data_put_binary(disp_data, pn_bytes(send_txn_id.size(),
                    reinterpret_cast<const char*>(send_txn_id.data())));
                pn_data_exit(disp_data);
            }

            // Encode message to buffer (using dynamic allocation variant)
            pn_rwbytes_t buf = {0, nullptr};
            ssize_t rc = pn_message_encode2(c_msg, &buf);
            if (rc < 0) {
                free(buf.start);
                error = "failed to encode message: " +
                    std::string(pn_error_text(pn_message_error(c_msg)));
            } else {
                // Send encoded bytes on the link (uses the current delivery)
                pn_link_send(c_link, buf.start, rc);
                pn_link_advance(c_link);
                free(buf.start);

                // Extract delivery tag for confirmation tracking
                pn_delivery_tag_t ptag = pn_delivery_tag(dlv);
                proton::binary tag(ptag.start, ptag.start + ptag.size);
                std::ostringstream oss;
                for (uint8_t b : tag) {
                    oss << std::hex << std::setfill('0') << std::setw(2)
                        << static_cast<int>(b);
                }
                tag_key = oss.str();
                {
                    std::lock_guard<std::mutex> lock(send_mutex_);
                    send_results_[tag_key] = SendResult{false, false, "", tag};
                }
                bytes_sent_ += rc;
                sent = true;
            }
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

    // Wait for broker confirmation (tracker accept/reject) with cooperative cancellation
    bool accepted = false;
    std::string tracker_error;
    proton::binary delivery_tag;
    {
        std::unique_lock<std::mutex> lock(send_mutex_);
        bool confirmed = waitWithCancel(lock, send_cv_, [&, this]() {
            auto it = send_results_.find(tag_key);
            if (it != send_results_.end() && it->second.done) {
                accepted = it->second.accepted;
                tracker_error = it->second.error;
                delivery_tag = it->second.tag;
                return true;
            }
            return !connected_.load();
        }, 30000, "AmqpConnection::send", xsink);

        // Clean up the tracking entry
        send_results_.erase(tag_key);

        if (*xsink) {
            return nullptr;
        }

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

    // NOTE: the Qpid Proton C++ delivery.modify() API does not accept parameters
    // for failed/undeliverable/annotations — these AMQP 1.0 MODIFIED outcome fields
    // are not exposed. The parameters are accepted at the Qore level for forward
    // compatibility but are currently ignored.
    // TODO: implement via low-level proton codec when Proton adds parameter support
    scheduleWork([delivery]() mutable {
        delivery.modify();
    }, xsink);
}

void QoreAmqpConnection::beginTransaction(ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }
    if (qore_check_cancel(xsink, "AmqpConnection::beginTransaction")) {
        return;
    }

    if (txn_active_) {
        xsink->raiseException("AMQP-TRANSACTION-ERROR", "a transaction is already active");
        return;
    }

    // Reset transaction state
    {
        std::lock_guard<std::mutex> lock(txn_mutex_);
        txn_coordinator_ready_ = false;
        txn_declare_done_ = false;
        txn_discharge_done_ = false;
        txn_error_.clear();
        txn_id_.clear();
    }

    // Step 1: Create coordinator sender link using C API (needed to set PN_COORDINATOR target)
    std::mutex mtx;
    std::condition_variable cv;
    bool link_done = false;
    std::string link_error;

    if (!scheduleWork([&, this]() {
        try {
            // Get the container's session from an existing link.
            // We need C API to create a coordinator (PN_COORDINATOR target
            // must be set BEFORE the link is opened / ATTACH is serialized).
            pn_session_t* c_sess = nullptr;
            {
                std::lock_guard<std::mutex> lk2(links_mutex_);
                if (!senders_.empty()) {
                    c_sess = pn_link_session(proton_unwrap<pn_link_t>(senders_.begin()->second));
                } else if (!receivers_.empty()) {
                    c_sess = pn_link_session(proton_unwrap<pn_link_t>(receivers_.begin()->second));
                }
            }
            if (!c_sess) {
                // No existing links — create a temp sender to obtain the session
                proton::sender temp = connection_.open_sender("_txn_session_probe");
                pn_link_t* tmp = proton_unwrap<pn_link_t>(temp);
                c_sess = pn_link_session(tmp);
                pn_link_close(tmp);
            }

            // Create coordinator sender via C API on the container's session.
            // Setting PN_COORDINATOR BEFORE pn_link_open() ensures the ATTACH
            // frame carries the correct target type.
            pn_link_t* c_link = pn_sender(c_sess, TXN_COORDINATOR_NAME);
            pn_terminus_set_type(pn_link_target(c_link), PN_COORDINATOR);
            pn_link_open(c_link);

            txn_coordinator_link_ = c_link;
        } catch (const std::exception& e) {
            link_error = e.what();
        }
        std::lock_guard<std::mutex> lk(mtx);
        link_done = true;
        cv.notify_all();
    }, xsink)) {
        return;
    }

    // Wait for link creation
    {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&]() { return link_done; });
    }
    if (!link_error.empty()) {
        xsink->raiseException("AMQP-TRANSACTION-ERROR",
            "failed to create transaction coordinator: %s", link_error.c_str());
        return;
    }

    // Step 2: Wait for coordinator to get credit (on_sendable)
    {
        std::unique_lock<std::mutex> lock(txn_mutex_);
        if (!waitWithCancel(lock, txn_cv_, [this]() {
            return txn_coordinator_ready_;
        }, 10000, "AmqpConnection::beginTransaction", xsink)) {
            if (!*xsink) {
                xsink->raiseException("AMQP-TRANSACTION-ERROR",
                    "timed out waiting for transaction coordinator credit");
            }
            return;
        }
    }

    // Step 3: Send Declare message on the coordinator
    if (!scheduleWork([this]() {
        // Build Declare message: body = described(0x31, list())
        pn_link_t* c_link = txn_coordinator_link_;
        static int declare_tag_counter = 0;
        std::string tag = "txn-declare-" + std::to_string(++declare_tag_counter);
        pn_delivery_t* d = pn_delivery(c_link, pn_dtag(tag.c_str(), tag.size()));

        // Encode Declare body: described type with descriptor 0x31 and empty list
        pn_message_t* msg = pn_message();
        pn_data_t* body = pn_message_body(msg);
        pn_data_put_described(body);
        pn_data_enter(body);
        pn_data_put_ulong(body, 0x31);  // amqp:declare:list descriptor
        pn_data_put_list(body);          // empty list (no global-id)
        pn_data_exit(body);

        // Encode and send
        char buf[512];
        size_t size = sizeof(buf);
        pn_message_encode(msg, buf, &size);
        pn_link_send(c_link, buf, size);
        pn_link_advance(c_link);
        pn_message_free(msg);
    }, xsink)) {
        return;
    }

    // Step 4: Wait for Declared response with txn-id
    {
        std::unique_lock<std::mutex> lock(txn_mutex_);
        if (!waitWithCancel(lock, txn_cv_, [this]() {
            return txn_declare_done_;
        }, 10000, "AmqpConnection::beginTransaction", xsink)) {
            if (!*xsink) {
                xsink->raiseException("AMQP-TRANSACTION-ERROR",
                    "timed out waiting for transaction declaration");
            }
            return;
        }

        if (!txn_error_.empty()) {
            std::string err = txn_error_;
            xsink->raiseException("AMQP-TRANSACTION-ERROR",
                "transaction declaration failed: %s", err.c_str());
            return;
        }
    }

    txn_active_ = true;
}

void QoreAmqpConnection::commitTransaction(ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    if (!txn_active_) {
        xsink->raiseException("AMQP-TRANSACTION-ERROR", "no active transaction");
        return;
    }

    discharge(false, xsink);
}

void QoreAmqpConnection::rollbackTransaction(ExceptionSink* xsink) {
    if (!checkConnected(xsink)) {
        return;
    }

    if (!txn_active_) {
        xsink->raiseException("AMQP-TRANSACTION-ERROR", "no active transaction");
        return;
    }

    discharge(true, xsink);
}

void QoreAmqpConnection::discharge(bool fail, ExceptionSink* xsink) {
    if (qore_check_cancel(xsink, "AmqpConnection::discharge")) {
        return;
    }

    // Reset discharge state
    {
        std::lock_guard<std::mutex> lock(txn_mutex_);
        txn_discharge_done_ = false;
        txn_error_.clear();
    }

    // Send Discharge message: described(0x32, list(txn-id, fail))
    proton::binary saved_txn_id = txn_id_;
    if (!scheduleWork([this, fail, saved_txn_id]() {
        pn_link_t* c_link = txn_coordinator_link_;
        static int discharge_tag_counter = 0;
        std::string tag = "txn-discharge-" + std::to_string(++discharge_tag_counter);
        pn_delivery_t* d = pn_delivery(c_link, pn_dtag(tag.c_str(), tag.size()));

        pn_message_t* msg = pn_message();
        pn_data_t* body = pn_message_body(msg);
        pn_data_put_described(body);
        pn_data_enter(body);
        pn_data_put_ulong(body, 0x32);  // amqp:discharge:list descriptor
        pn_data_put_list(body);
        pn_data_enter(body);
        pn_data_put_binary(body, pn_bytes(saved_txn_id.size(),
            reinterpret_cast<const char*>(saved_txn_id.data())));
        pn_data_put_bool(body, fail);
        pn_data_exit(body);
        pn_data_exit(body);

        char buf[512];
        size_t size = sizeof(buf);
        pn_message_encode(msg, buf, &size);
        pn_link_send(c_link, buf, size);
        pn_link_advance(c_link);
        pn_message_free(msg);
    }, xsink)) {
        return;
    }

    // Wait for discharge confirmation
    {
        std::unique_lock<std::mutex> lock(txn_mutex_);
        if (!waitWithCancel(lock, txn_cv_, [this]() {
            return txn_discharge_done_;
        }, 10000, "AmqpConnection::discharge", xsink)) {
            if (!*xsink) {
                xsink->raiseException("AMQP-TRANSACTION-ERROR",
                    "timed out waiting for transaction %s",
                    fail ? "rollback" : "commit");
            }
            // Transaction state is indeterminate — mark as inactive
            txn_active_ = false;
            txn_id_.clear();
            return;
        }

        if (!txn_error_.empty()) {
            std::string err = txn_error_;
            txn_active_ = false;
            txn_id_.clear();
            xsink->raiseException("AMQP-TRANSACTION-ERROR",
                "transaction %s failed: %s", fail ? "rollback" : "commit",
                err.c_str());
            return;
        }
    }

    txn_active_ = false;
    txn_id_.clear();
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
    if (qore_check_cancel(xsink, "AmqpConnection::createDurableReceiver")) {
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
            name = r.name();
            {
                std::lock_guard<std::mutex> lock(links_mutex_);
                receivers_[name] = r;
            }
            // Register for reconnect recovery (durable)
            {
                std::lock_guard<std::mutex> lock(registry_mutex_);
                link_registry_.push_back(LinkInfo{addr, false, true, std::string(subscription_name)});
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
    if (!waitWithCancel(lock, cv, [&done]() { return done; },
            30000, "AmqpConnection::createDurableReceiver", xsink)) {
        if (!*xsink) {
            xsink->raiseException("AMQP-RECEIVER-ERROR",
                "timed out creating durable receiver for '%s'", address);
        }
        return nullptr;
    }

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
    if (qore_check_cancel(xsink, "AmqpConnection::unsubscribeDurable")) {
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
    if (!waitWithCancel(lock, cv, [&done]() { return done; },
            30000, "AmqpConnection::unsubscribeDurable", xsink)) {
        if (!*xsink) {
            xsink->raiseException("AMQP-SUBSCRIPTION-ERROR",
                "timed out unsubscribing durable '%s'", subscription_name);
        }
        return;
    }

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

    return parseQueryResponse(*result, hashdeclAmqpAddressInfo, buildAddressInfo, xsink);
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

    ReferenceHolder<QoreListNode> all(
        parseQueryResponse(*result, hashdeclAmqpQueueInfo, buildQueueInfo, xsink), xsink);
    if (*xsink || !all) {
        return new QoreListNode(hashdeclAmqpQueueInfo->getTypeInfo());
    }

    // Client-side filter by address if requested
    if (address && *address) {
        ReferenceHolder<QoreListNode> filtered(
            new QoreListNode(hashdeclAmqpQueueInfo->getTypeInfo()), xsink);
        std::string filter_addr(address);
        for (size_t i = 0; i < all->size(); ++i) {
            QoreValue elem = all->retrieveEntry(i);
            if (elem.getType() == NT_HASH) {
                std::string addr = getStringVal(elem.get<const QoreHashNode>(), "address");
                if (addr == filter_addr) {
                    elem.refSelf();
                    filtered->push(elem, xsink);
                }
            }
        }
        return filtered.release();
    }
    return all.release();
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

    return buildAddressInfo(*result, xsink);
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

    return buildQueueInfo(*result, xsink);
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
            if (!waitWithCancel(lock2, cv, [&done]() { return done; },
                    10000, "AmqpConnection::managementRequest", xsink)) {
                if (!*xsink) {
                    xsink->raiseException("AMQP-MANAGEMENT-ERROR",
                        "timed out initializing management");
                }
                return nullptr;
            }

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

    // Wait for response with timeout and cooperative cancellation
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    std::string reply_addr = mgmt_receiver_.source().address();

    while (!done) {
        // Check cooperative cancellation
        if (qore_check_cancel(xsink, "AmqpConnection::managementRequest")) {
            return nullptr;
        }

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
            std::chrono::steady_clock::now()
                + std::chrono::milliseconds(QORE_IO_POLL_INTERVAL_MS));
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

QoreHashNode* QoreAmqpConnection::getStatistics(ExceptionSink* xsink) {
    ReferenceHolder<QoreHashNode> stats(new QoreHashNode(hashdeclAmqpConnectionStats, xsink), xsink);
    stats->setKeyValue("messages_sent", messages_sent_.load(), xsink);
    stats->setKeyValue("messages_received", messages_received_.load(), xsink);
    stats->setKeyValue("bytes_sent", bytes_sent_.load(), xsink);
    stats->setKeyValue("bytes_received", bytes_received_.load(), xsink);
    stats->setKeyValue("errors", errors_.load(), xsink);

    {
        std::lock_guard<std::mutex> lock(links_mutex_);
        stats->setKeyValue("link_count",
            static_cast<int64>(senders_.size() + receivers_.size()), xsink);
    }

    if (connected_since_epoch_us_ > 0) {
        stats->setKeyValue("connected_since",
            DateTimeNode::makeAbsolute(0, connected_since_epoch_us_ / 1000000,
                static_cast<int>(connected_since_epoch_us_ % 1000000)),
            xsink);
    }

    if (*xsink) {
        return nullptr;
    }
    return stats.release();
}

void QoreAmqpConnection::pushEvent(const std::string& event_id, const std::string& err) {
    std::lock_guard<std::mutex> lock(event_mutex_);
    if (event_queue_.size() >= MAX_EVENT_QUEUE) {
        event_queue_.pop();  // drop oldest
    }
    auto now = std::chrono::system_clock::now();
    int64 ts = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
    event_queue_.push(ConnectionEvent{event_id, err, ts});
}

QoreListNode* QoreAmqpConnection::getConnectionEvents(ExceptionSink* xsink) {
    ReferenceHolder<QoreListNode> list(new QoreListNode(autoHashTypeInfo), xsink);
    std::lock_guard<std::mutex> lock(event_mutex_);
    while (!event_queue_.empty()) {
        ConnectionEvent& ev = event_queue_.front();
        ReferenceHolder<QoreHashNode> h(new QoreHashNode(autoTypeInfo), xsink);
        h->setKeyValue("event", new QoreStringNode(ev.event_id), xsink);
        h->setKeyValue("timestamp",
            DateTimeNode::makeAbsolute(0, ev.timestamp_us / 1000000,
                static_cast<int>(ev.timestamp_us % 1000000)), xsink);
        if (!ev.error.empty()) {
            h->setKeyValue("error", new QoreStringNode(ev.error), xsink);
        }
        list->push(h.release(), xsink);
        event_queue_.pop();
    }
    if (*xsink) {
        return nullptr;
    }
    return list.release();
}

void QoreAmqpConnection::setAutoRecoverLinks(bool recover) {
    auto_recover_links_ = recover;
}

std::string QoreAmqpConnection::generateLinkName(const char* prefix) {
    std::lock_guard<std::mutex> lock(links_mutex_);
    return std::string(prefix) + "-" + std::to_string(++link_counter_);
}

std::string QoreAmqpConnection::deliveryTagKey(const proton::delivery& d) {
    proton::binary tag = d.tag();
    std::ostringstream oss;
    for (uint8_t b : tag) {
        oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(b);
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
        oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(data[i]);
    }
    return oss.str();
}
