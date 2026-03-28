/* -*- mode: c++; indent-tabs-mode: nil -*- */
/** @file QoreAmqpHelper.cpp AMQP utility function implementations */
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

#include "QoreAmqpHelper.h"

#include <proton/codec/decoder.hpp>
#include <proton/codec/encoder.hpp>

QoreValue QoreAmqpHelper::protonToQore(const proton::value& val, ExceptionSink* xsink) {
    if (val.empty()) {
        return QoreValue();
    }

    proton::type_id type = val.type();
    switch (type) {
        case proton::NULL_TYPE:
            return QoreValue();

        case proton::BOOLEAN:
            return QoreValue(proton::get<bool>(val));

        case proton::BYTE:
            return QoreValue((int64)proton::get<int8_t>(val));

        case proton::SHORT:
            return QoreValue((int64)proton::get<int16_t>(val));

        case proton::INT:
            return QoreValue((int64)proton::get<int32_t>(val));

        case proton::LONG:
            return QoreValue((int64)proton::get<int64_t>(val));

        case proton::UBYTE:
            return QoreValue((int64)proton::get<uint8_t>(val));

        case proton::USHORT:
            return QoreValue((int64)proton::get<uint16_t>(val));

        case proton::UINT:
            return QoreValue((int64)proton::get<uint32_t>(val));

        case proton::ULONG:
            return QoreValue((int64)proton::get<uint64_t>(val));

        case proton::FLOAT:
            return QoreValue((double)proton::get<float>(val));

        case proton::DOUBLE:
            return QoreValue(proton::get<double>(val));

        case proton::STRING:
            return new QoreStringNode(proton::get<std::string>(val));

        case proton::SYMBOL:
            return new QoreStringNode(proton::get<proton::symbol>(val));

        case proton::BINARY: {
            proton::binary bin = proton::get<proton::binary>(val);
            return protonBinaryToQore(bin);
        }

        case proton::TIMESTAMP:
            return timestampToDate(proton::get<proton::timestamp>(val));

        case proton::UUID:
            return uuidToString(proton::get<proton::uuid>(val));

        case proton::MAP:
            return protonMapToHash(val, xsink);

        case proton::LIST:
        case proton::ARRAY:
            return protonListToQore(val, xsink);

        case proton::DESCRIBED:
        case proton::CHAR:
        case proton::DECIMAL32:
        case proton::DECIMAL64:
        case proton::DECIMAL128:
        default:
            // For unsupported types, convert to string representation
            return new QoreStringNode(proton::to_string(val));
    }
}

proton::value QoreAmqpHelper::qoreToProton(const QoreValue& val, ExceptionSink* xsink) {
    if (val.isNullOrNothing()) {
        return proton::value();
    }

    switch (val.getType()) {
        case NT_BOOLEAN:
            return proton::value(val.getAsBool());

        case NT_INT:
            return proton::value(val.getAsBigInt());

        case NT_FLOAT:
            return proton::value(val.getAsFloat());

        case NT_STRING: {
            const QoreStringNode* str = val.get<const QoreStringNode>();
            return proton::value(std::string(str->c_str(), str->size()));
        }

        case NT_BINARY: {
            const BinaryNode* bin = val.get<const BinaryNode>();
            return proton::value(qoreBinaryToProton(bin));
        }

        case NT_DATE: {
            const DateTimeNode* dt = val.get<const DateTimeNode>();
            return proton::value(dateToTimestamp(dt));
        }

        case NT_HASH: {
            const QoreHashNode* hash = val.get<const QoreHashNode>();
            return hashToProtonMap(hash, xsink);
        }

        case NT_LIST: {
            const QoreListNode* list = val.get<const QoreListNode>();
            return listToProton(list, xsink);
        }

        case NT_NUMBER: {
            // Convert Qore number to double for AMQP
            return proton::value(val.getAsFloat());
        }

        default:
            xsink->raiseException("AMQP-TYPE-ERROR", "unsupported Qore type '%s' for AMQP conversion",
                val.getFullTypeName());
            return proton::value();
    }
}

QoreValue QoreAmqpHelper::scalarToQore(const proton::scalar& sc, ExceptionSink* xsink) {
    if (sc.empty()) {
        return QoreValue();
    }

    proton::type_id type = sc.type();
    switch (type) {
        case proton::NULL_TYPE:
            return QoreValue();

        case proton::BOOLEAN:
            return QoreValue(proton::get<bool>(sc));

        case proton::BYTE:
            return QoreValue((int64)proton::get<int8_t>(sc));

        case proton::SHORT:
            return QoreValue((int64)proton::get<int16_t>(sc));

        case proton::INT:
            return QoreValue((int64)proton::get<int32_t>(sc));

        case proton::LONG:
            return QoreValue((int64)proton::get<int64_t>(sc));

        case proton::UBYTE:
            return QoreValue((int64)proton::get<uint8_t>(sc));

        case proton::USHORT:
            return QoreValue((int64)proton::get<uint16_t>(sc));

        case proton::UINT:
            return QoreValue((int64)proton::get<uint32_t>(sc));

        case proton::ULONG:
            return QoreValue((int64)proton::get<uint64_t>(sc));

        case proton::FLOAT:
            return QoreValue((double)proton::get<float>(sc));

        case proton::DOUBLE:
            return QoreValue(proton::get<double>(sc));

        case proton::STRING:
            return new QoreStringNode(proton::get<std::string>(sc));

        case proton::SYMBOL:
            return new QoreStringNode(proton::get<proton::symbol>(sc));

        case proton::BINARY: {
            proton::binary bin = proton::get<proton::binary>(sc);
            return protonBinaryToQore(bin);
        }

        case proton::TIMESTAMP:
            return timestampToDate(proton::get<proton::timestamp>(sc));

        case proton::UUID:
            return uuidToString(proton::get<proton::uuid>(sc));

        default:
            return new QoreStringNode(proton::to_string(sc));
    }
}

QoreValue QoreAmqpHelper::scalarBaseToQore(const proton::scalar_base& sb, ExceptionSink* xsink) {
    if (sb.empty()) {
        return QoreValue();
    }

    proton::type_id type = sb.type();
    switch (type) {
        case proton::NULL_TYPE:
            return QoreValue();

        case proton::BOOLEAN:
            return QoreValue(proton::internal::get<bool>(sb));

        case proton::BYTE:
            return QoreValue((int64)proton::internal::get<int8_t>(sb));

        case proton::SHORT:
            return QoreValue((int64)proton::internal::get<int16_t>(sb));

        case proton::INT:
            return QoreValue((int64)proton::internal::get<int32_t>(sb));

        case proton::LONG:
            return QoreValue((int64)proton::internal::get<int64_t>(sb));

        case proton::UBYTE:
            return QoreValue((int64)proton::internal::get<uint8_t>(sb));

        case proton::USHORT:
            return QoreValue((int64)proton::internal::get<uint16_t>(sb));

        case proton::UINT:
            return QoreValue((int64)proton::internal::get<uint32_t>(sb));

        case proton::ULONG:
            return QoreValue((int64)proton::internal::get<uint64_t>(sb));

        case proton::FLOAT:
            return QoreValue((double)proton::internal::get<float>(sb));

        case proton::DOUBLE:
            return QoreValue(proton::internal::get<double>(sb));

        case proton::STRING:
            return new QoreStringNode(proton::internal::get<std::string>(sb));

        case proton::SYMBOL:
            return new QoreStringNode(proton::internal::get<proton::symbol>(sb));

        case proton::BINARY: {
            proton::binary bin = proton::internal::get<proton::binary>(sb);
            return protonBinaryToQore(bin);
        }

        case proton::TIMESTAMP:
            return timestampToDate(proton::internal::get<proton::timestamp>(sb));

        case proton::UUID:
            return uuidToString(proton::internal::get<proton::uuid>(sb));

        default:
            // Convert to string for unsupported types
            return new QoreStringNode("<unsupported AMQP type>");
    }
}

proton::scalar QoreAmqpHelper::qoreToScalar(const QoreValue& val, ExceptionSink* xsink) {
    if (val.isNullOrNothing()) {
        return proton::scalar();
    }

    switch (val.getType()) {
        case NT_BOOLEAN:
            return proton::scalar(val.getAsBool());

        case NT_INT:
            return proton::scalar(val.getAsBigInt());

        case NT_FLOAT:
            return proton::scalar(val.getAsFloat());

        case NT_STRING: {
            const QoreStringNode* str = val.get<const QoreStringNode>();
            return proton::scalar(std::string(str->c_str(), str->size()));
        }

        case NT_BINARY: {
            const BinaryNode* bin = val.get<const BinaryNode>();
            return proton::scalar(qoreBinaryToProton(bin));
        }

        case NT_DATE: {
            const DateTimeNode* dt = val.get<const DateTimeNode>();
            return proton::scalar(dateToTimestamp(dt));
        }

        default:
            xsink->raiseException("AMQP-TYPE-ERROR", "unsupported Qore type '%s' for AMQP scalar",
                val.getFullTypeName());
            return proton::scalar();
    }
}

proton::message_id QoreAmqpHelper::qoreToMessageId(const QoreValue& val, ExceptionSink* xsink) {
    if (val.isNullOrNothing()) {
        return proton::message_id();
    }

    switch (val.getType()) {
        case NT_INT: {
            // message_id only accepts uint64_t for integers
            return proton::message_id(static_cast<uint64_t>(val.getAsBigInt()));
        }

        case NT_STRING: {
            const QoreStringNode* str = val.get<const QoreStringNode>();
            return proton::message_id(std::string(str->c_str(), str->size()));
        }

        case NT_BINARY: {
            const BinaryNode* bin = val.get<const BinaryNode>();
            return proton::message_id(qoreBinaryToProton(bin));
        }

        default:
            xsink->raiseException("AMQP-TYPE-ERROR",
                "unsupported Qore type '%s' for AMQP message_id (expected string, int, or binary)",
                val.getFullTypeName());
            return proton::message_id();
    }
}

QoreHashNode* QoreAmqpHelper::protonMapToHash(const proton::value& val, ExceptionSink* xsink) {
    ReferenceHolder<QoreHashNode> hash(new QoreHashNode(autoTypeInfo), xsink);

    try {
        proton::codec::decoder d(val);
        proton::codec::start s;
        d >> s;

        for (uint32_t i = 0; i < s.size / 2; ++i) {
            proton::value key;
            proton::value value;
            d >> key >> value;

            std::string key_str;
            if (key.type() == proton::STRING) {
                key_str = proton::get<std::string>(key);
            } else if (key.type() == proton::SYMBOL) {
                key_str = proton::get<proton::symbol>(key);
            } else {
                key_str = proton::to_string(key);
            }

            QoreValue qv = protonToQore(value, xsink);
            if (*xsink) {
                return nullptr;
            }
            hash->setKeyValue(key_str.c_str(), qv, xsink);
            if (*xsink) {
                return nullptr;
            }
        }
    } catch (const std::exception& e) {
        xsink->raiseException("AMQP-DECODE-ERROR", "failed to decode AMQP map: %s", e.what());
        return nullptr;
    }

    return hash.release();
}

proton::value QoreAmqpHelper::hashToProtonMap(const QoreHashNode* hash, ExceptionSink* xsink) {
    try {
        proton::value result;
        proton::codec::encoder enc(result);
        enc << proton::codec::start::map();

        ConstHashIterator hi(hash);
        while (hi.next()) {
            enc << std::string(hi.getKey());
            proton::value pv = qoreToProton(hi.get(), xsink);
            if (*xsink) {
                return proton::value();
            }
            enc << pv;
        }

        enc << proton::codec::finish();
        return result;
    } catch (const std::exception& e) {
        xsink->raiseException("AMQP-ENCODE-ERROR", "failed to encode AMQP map: %s", e.what());
        return proton::value();
    }
}

QoreListNode* QoreAmqpHelper::protonListToQore(const proton::value& val, ExceptionSink* xsink) {
    ReferenceHolder<QoreListNode> list(new QoreListNode(autoTypeInfo), xsink);

    try {
        proton::codec::decoder d(val);
        proton::codec::start s;
        d >> s;

        for (uint32_t i = 0; i < s.size; ++i) {
            proton::value elem;
            d >> elem;

            QoreValue qv = protonToQore(elem, xsink);
            if (*xsink) {
                return nullptr;
            }
            list->push(qv, xsink);
            if (*xsink) {
                return nullptr;
            }
        }
    } catch (const std::exception& e) {
        xsink->raiseException("AMQP-DECODE-ERROR", "failed to decode AMQP list: %s", e.what());
        return nullptr;
    }

    return list.release();
}

proton::value QoreAmqpHelper::listToProton(const QoreListNode* list, ExceptionSink* xsink) {
    try {
        proton::value result;
        proton::codec::encoder enc(result);
        enc << proton::codec::start::list();

        for (size_t i = 0; i < list->size(); ++i) {
            proton::value pv = qoreToProton(list->retrieveEntry(i), xsink);
            if (*xsink) {
                return proton::value();
            }
            enc << pv;
        }

        enc << proton::codec::finish();
        return result;
    } catch (const std::exception& e) {
        xsink->raiseException("AMQP-ENCODE-ERROR", "failed to encode AMQP list: %s", e.what());
        return proton::value();
    }
}

DateTimeNode* QoreAmqpHelper::timestampToDate(proton::timestamp ts) {
    int64 ms = ts.milliseconds();
    int64 secs = ms / 1000;
    int us = static_cast<int>((ms % 1000) * 1000);
    return DateTimeNode::makeAbsolute(0, secs, us);
}

proton::timestamp QoreAmqpHelper::dateToTimestamp(const DateTimeNode* dt) {
    int64 epoch_secs = dt->getEpochSecondsUTC();
    int us = dt->getMicrosecond();
    int64 ms = epoch_secs * 1000 + us / 1000;
    return proton::timestamp(ms);
}

BinaryNode* QoreAmqpHelper::protonBinaryToQore(const proton::binary& bin) {
    BinaryNode* node = new BinaryNode;
    if (!bin.empty()) {
        node->append(bin.data(), bin.size());
    }
    return node;
}

proton::binary QoreAmqpHelper::qoreBinaryToProton(const BinaryNode* bin) {
    if (!bin || !bin->size()) {
        return proton::binary();
    }
    const uint8_t* data = reinterpret_cast<const uint8_t*>(bin->getPtr());
    return proton::binary(std::vector<uint8_t>(data, data + bin->size()));
}

QoreStringNode* QoreAmqpHelper::uuidToString(const proton::uuid& uuid) {
    // Format as standard UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    QoreStringNode* str = new QoreStringNode;
    str->sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        uuid[0], uuid[1], uuid[2], uuid[3],
        uuid[4], uuid[5],
        uuid[6], uuid[7],
        uuid[8], uuid[9],
        uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15]);
    return str;
}

QoreHashNode* QoreAmqpHelper::propertyMapToHash(const proton::message::property_map& pm,
        ExceptionSink* xsink) {
    if (pm.empty()) {
        return nullptr;
    }
    // Decode the property map value (same approach as annotationMapToHash)
    const proton::value& val = const_cast<proton::message::property_map&>(pm).value();
    return protonMapToHash(val, xsink);
}

QoreHashNode* QoreAmqpHelper::annotationMapToHash(const proton::message::annotation_map& am,
        ExceptionSink* xsink) {
    if (am.empty()) {
        return nullptr;
    }
    // Decode the annotation map value manually since proton::map has no iterators
    const proton::value& val = const_cast<proton::message::annotation_map&>(am).value();
    return protonMapToHash(val, xsink);
}

std::string QoreAmqpHelper::formatError(const std::string& prefix, const std::string& msg) {
    return prefix + ": " + msg;
}

void QoreAmqpHelper::parseUrlHostPort(const std::string& url, std::string& host, int& port) {
    // URL format: amqp[s]://[user[:password]@]host[:port][/path]
    bool tls = (url.substr(0, 5) == "amqps");
    port = tls ? 5671 : 5672;

    // Find the authority part (after "://")
    size_t auth_start = url.find("://");
    if (auth_start == std::string::npos) {
        host = "localhost";
        return;
    }
    auth_start += 3;

    // Find end of authority (before "/" or end of string)
    size_t auth_end = url.find('/', auth_start);
    if (auth_end == std::string::npos) {
        auth_end = url.size();
    }

    std::string authority = url.substr(auth_start, auth_end - auth_start);

    // Strip userinfo (user:password@)
    size_t at_pos = authority.rfind('@');
    if (at_pos != std::string::npos) {
        authority = authority.substr(at_pos + 1);
    }

    // Check for IPv6 literal [host]:port
    if (!authority.empty() && authority[0] == '[') {
        size_t bracket_end = authority.find(']');
        if (bracket_end != std::string::npos) {
            host = authority.substr(1, bracket_end - 1);
            if (bracket_end + 1 < authority.size() && authority[bracket_end + 1] == ':') {
                port = std::stoi(authority.substr(bracket_end + 2));
            }
            return;
        }
    }

    // host:port or just host
    size_t colon_pos = authority.rfind(':');
    if (colon_pos != std::string::npos) {
        host = authority.substr(0, colon_pos);
        try {
            port = std::stoi(authority.substr(colon_pos + 1));
        } catch (...) {
            // Invalid port — use default
        }
    } else {
        host = authority;
    }

    if (host.empty()) {
        host = "localhost";
    }
}
