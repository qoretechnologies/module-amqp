/* -*- mode: c++; indent-tabs-mode: nil -*- */
/** @file QoreAmqpMessage.cpp QoreAmqpMessage implementation */
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

#include "QoreAmqpMessage.h"

QoreAmqpMessage::QoreAmqpMessage(const QoreValue& body, const QoreHashNode* properties,
        ExceptionSink* xsink) {
    // Set the message body
    if (!body.isNullOrNothing()) {
        msg.body(QoreAmqpHelper::qoreToProton(body, xsink));
        if (*xsink) {
            return;
        }
        // Store the original body for exact round-trip
        qore_body = body.refSelf();
    }

    // Apply properties if provided
    if (properties) {
        applyProperties(properties, xsink);
    }
}

QoreAmqpMessage::QoreAmqpMessage(const proton::message& src, const proton::binary& delivery_tag,
        ExceptionSink* xsink) : msg(src), delivery_tag_(delivery_tag) {
    // Convert the proton body to a Qore value for caching
    qore_body = QoreAmqpHelper::protonToQore(msg.body(), xsink);
}

QoreValue QoreAmqpMessage::getBody(ExceptionSink* xsink) const {
    return qore_body.refSelf();
}

QoreHashNode* QoreAmqpMessage::getProperties(ExceptionSink* xsink) const {
    ReferenceHolder<QoreHashNode> props(new QoreHashNode(hashdeclAmqpMessageProperties, xsink), xsink);

    // message_id
    if (!msg.id().empty()) {
        QoreValue id = QoreAmqpHelper::scalarBaseToQore(msg.id(), xsink);
        if (*xsink) {
            return nullptr;
        }
        props->setKeyValue("message_id", id, xsink);
    }

    // correlation_id
    if (!msg.correlation_id().empty()) {
        QoreValue cid = QoreAmqpHelper::scalarBaseToQore(msg.correlation_id(), xsink);
        if (*xsink) {
            return nullptr;
        }
        props->setKeyValue("correlation_id", cid, xsink);
    }

    // reply_to
    if (!msg.reply_to().empty()) {
        props->setKeyValue("reply_to", new QoreStringNode(msg.reply_to()), xsink);
    }

    // content_type
    if (!msg.content_type().empty()) {
        props->setKeyValue("content_type", new QoreStringNode(msg.content_type()), xsink);
    }

    // content_encoding
    if (!msg.content_encoding().empty()) {
        props->setKeyValue("content_encoding", new QoreStringNode(msg.content_encoding()), xsink);
    }

    // subject
    if (!msg.subject().empty()) {
        props->setKeyValue("subject", new QoreStringNode(msg.subject()), xsink);
    }

    // to
    if (!msg.to().empty()) {
        props->setKeyValue("to", new QoreStringNode(msg.to()), xsink);
    }

    // group_id
    if (!msg.group_id().empty()) {
        props->setKeyValue("group_id", new QoreStringNode(msg.group_id()), xsink);
    }

    // group_sequence
    if (msg.group_sequence() != 0) {
        props->setKeyValue("group_sequence", static_cast<int64>(msg.group_sequence()), xsink);
    }

    // reply_to_group_id
    if (!msg.reply_to_group_id().empty()) {
        props->setKeyValue("reply_to_group_id", new QoreStringNode(msg.reply_to_group_id()), xsink);
    }

    // expiry_time
    if (msg.expiry_time().milliseconds() != 0) {
        props->setKeyValue("expiry_time",
            QoreAmqpHelper::timestampToDate(msg.expiry_time()), xsink);
    }

    // creation_time
    if (msg.creation_time().milliseconds() != 0) {
        props->setKeyValue("creation_time",
            QoreAmqpHelper::timestampToDate(msg.creation_time()), xsink);
    }

    // application_properties
    const proton::message::property_map& ap = msg.properties();
    if (!ap.empty()) {
        QoreHashNode* app_props = QoreAmqpHelper::propertyMapToHash(ap, xsink);
        if (*xsink) {
            return nullptr;
        }
        if (app_props) {
            props->setKeyValue("application_properties", app_props, xsink);
        }
    }

    // message_annotations
    const proton::message::annotation_map& ma = msg.message_annotations();
    if (!ma.empty()) {
        QoreHashNode* ann = QoreAmqpHelper::annotationMapToHash(ma, xsink);
        if (*xsink) {
            return nullptr;
        }
        if (ann) {
            props->setKeyValue("message_annotations", ann, xsink);
        }
    }

    // delivery_annotations
    const proton::message::annotation_map& da = msg.delivery_annotations();
    if (!da.empty()) {
        QoreHashNode* ann = QoreAmqpHelper::annotationMapToHash(da, xsink);
        if (*xsink) {
            return nullptr;
        }
        if (ann) {
            props->setKeyValue("delivery_annotations", ann, xsink);
        }
    }

    return props.release();
}

QoreHashNode* QoreAmqpMessage::getApplicationProperties(ExceptionSink* xsink) const {
    const proton::message::property_map& ap = msg.properties();
    if (ap.empty()) {
        return nullptr;
    }
    return QoreAmqpHelper::propertyMapToHash(ap, xsink);
}

QoreValue QoreAmqpMessage::getContentType(ExceptionSink* xsink) const {
    if (msg.content_type().empty()) {
        return QoreValue();
    }
    return new QoreStringNode(msg.content_type());
}

QoreValue QoreAmqpMessage::getMessageId(ExceptionSink* xsink) const {
    if (msg.id().empty()) {
        return QoreValue();
    }
    return QoreAmqpHelper::scalarBaseToQore(msg.id(), xsink);
}

QoreValue QoreAmqpMessage::getCorrelationId(ExceptionSink* xsink) const {
    if (msg.correlation_id().empty()) {
        return QoreValue();
    }
    return QoreAmqpHelper::scalarBaseToQore(msg.correlation_id(), xsink);
}

QoreValue QoreAmqpMessage::getReplyTo(ExceptionSink* xsink) const {
    if (msg.reply_to().empty()) {
        return QoreValue();
    }
    return new QoreStringNode(msg.reply_to());
}

QoreValue QoreAmqpMessage::getSubject(ExceptionSink* xsink) const {
    if (msg.subject().empty()) {
        return QoreValue();
    }
    return new QoreStringNode(msg.subject());
}

BinaryNode* QoreAmqpMessage::getDeliveryTag() const {
    if (delivery_tag_.empty()) {
        return nullptr;
    }
    BinaryNode* tag = new BinaryNode;
    tag->append(delivery_tag_.data(), delivery_tag_.size());
    return tag;
}

void QoreAmqpMessage::applyProperties(const QoreHashNode* properties, ExceptionSink* xsink) {
    // message_id
    QoreValue v = properties->getKeyValue("message_id");
    if (!v.isNullOrNothing()) {
        msg.id(QoreAmqpHelper::qoreToMessageId(v, xsink));
        if (*xsink) {
            return;
        }
    }

    // correlation_id
    v = properties->getKeyValue("correlation_id");
    if (!v.isNullOrNothing()) {
        msg.correlation_id(QoreAmqpHelper::qoreToMessageId(v, xsink));
        if (*xsink) {
            return;
        }
    }

    // reply_to
    v = properties->getKeyValue("reply_to");
    if (!v.isNullOrNothing()) {
        const QoreStringNode* str = v.get<const QoreStringNode>();
        if (str) {
            msg.reply_to(std::string(str->c_str()));
        }
    }

    // content_type
    v = properties->getKeyValue("content_type");
    if (!v.isNullOrNothing()) {
        const QoreStringNode* str = v.get<const QoreStringNode>();
        if (str) {
            msg.content_type(std::string(str->c_str()));
        }
    }

    // content_encoding
    v = properties->getKeyValue("content_encoding");
    if (!v.isNullOrNothing()) {
        const QoreStringNode* str = v.get<const QoreStringNode>();
        if (str) {
            msg.content_encoding(std::string(str->c_str()));
        }
    }

    // subject
    v = properties->getKeyValue("subject");
    if (!v.isNullOrNothing()) {
        const QoreStringNode* str = v.get<const QoreStringNode>();
        if (str) {
            msg.subject(std::string(str->c_str()));
        }
    }

    // to
    v = properties->getKeyValue("to");
    if (!v.isNullOrNothing()) {
        const QoreStringNode* str = v.get<const QoreStringNode>();
        if (str) {
            msg.to(std::string(str->c_str()));
        }
    }

    // group_id
    v = properties->getKeyValue("group_id");
    if (!v.isNullOrNothing()) {
        const QoreStringNode* str = v.get<const QoreStringNode>();
        if (str) {
            msg.group_id(std::string(str->c_str()));
        }
    }

    // group_sequence
    v = properties->getKeyValue("group_sequence");
    if (!v.isNullOrNothing()) {
        msg.group_sequence((int32_t)v.getAsBigInt());
    }

    // reply_to_group_id
    v = properties->getKeyValue("reply_to_group_id");
    if (!v.isNullOrNothing()) {
        const QoreStringNode* str = v.get<const QoreStringNode>();
        if (str) {
            msg.reply_to_group_id(std::string(str->c_str()));
        }
    }

    // expiry_time
    v = properties->getKeyValue("expiry_time");
    if (!v.isNullOrNothing()) {
        const DateTimeNode* dt = v.get<const DateTimeNode>();
        if (dt) {
            msg.expiry_time(QoreAmqpHelper::dateToTimestamp(dt));
        }
    }

    // creation_time
    v = properties->getKeyValue("creation_time");
    if (!v.isNullOrNothing()) {
        const DateTimeNode* dt = v.get<const DateTimeNode>();
        if (dt) {
            msg.creation_time(QoreAmqpHelper::dateToTimestamp(dt));
        }
    }

    // application_properties
    v = properties->getKeyValue("application_properties");
    if (!v.isNullOrNothing()) {
        const QoreHashNode* ap = v.get<const QoreHashNode>();
        if (ap) {
            ConstHashIterator hi(ap);
            while (hi.next()) {
                proton::scalar sv = QoreAmqpHelper::qoreToScalar(hi.get(), xsink);
                if (*xsink) {
                    return;
                }
                msg.properties().put(std::string(hi.getKey()), sv);
            }
        }
    }

    // message_annotations
    v = properties->getKeyValue("message_annotations");
    if (!v.isNullOrNothing()) {
        const QoreHashNode* ma = v.get<const QoreHashNode>();
        if (ma) {
            ConstHashIterator hi(ma);
            while (hi.next()) {
                proton::scalar sv = QoreAmqpHelper::qoreToScalar(hi.get(), xsink);
                if (*xsink) {
                    return;
                }
                msg.message_annotations().put(proton::symbol(std::string(hi.getKey())), sv);
            }
        }
    }

    // delivery_annotations
    v = properties->getKeyValue("delivery_annotations");
    if (!v.isNullOrNothing()) {
        const QoreHashNode* da = v.get<const QoreHashNode>();
        if (da) {
            ConstHashIterator hi(da);
            while (hi.next()) {
                proton::scalar sv = QoreAmqpHelper::qoreToScalar(hi.get(), xsink);
                if (*xsink) {
                    return;
                }
                msg.delivery_annotations().put(proton::symbol(std::string(hi.getKey())), sv);
            }
        }
    }
}
