/* -*- mode: c++; indent-tabs-mode: nil -*- */
/** @file QoreAmqpMessage.h QoreAmqpMessage class definition */
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

#ifndef _QORE_AMQP_MESSAGE_H
#define _QORE_AMQP_MESSAGE_H

#include "amqp-module.h"
#include "QoreAmqpHelper.h"

#include <proton/message.hpp>

#include <memory>

//! Wraps a proton::message for use as Qore private data
class QoreAmqpMessage : public AbstractPrivateData {
public:
    //! Constructor: create from Qore body and optional properties
    DLLLOCAL QoreAmqpMessage(const QoreValue& body, const QoreHashNode* properties, ExceptionSink* xsink);

    //! Constructor: create from a received proton::message
    DLLLOCAL QoreAmqpMessage(const proton::message& msg, ExceptionSink* xsink);

    DLLLOCAL ~QoreAmqpMessage() override {
        qore_body.discard(nullptr);
    }

    //! Get the message body as a QoreValue
    DLLLOCAL QoreValue getBody(ExceptionSink* xsink) const;

    //! Get all message properties as a hash
    DLLLOCAL QoreHashNode* getProperties(ExceptionSink* xsink) const;

    //! Get application properties
    DLLLOCAL QoreHashNode* getApplicationProperties(ExceptionSink* xsink) const;

    //! Get content type
    DLLLOCAL QoreValue getContentType(ExceptionSink* xsink) const;

    //! Get message ID
    DLLLOCAL QoreValue getMessageId(ExceptionSink* xsink) const;

    //! Get correlation ID
    DLLLOCAL QoreValue getCorrelationId(ExceptionSink* xsink) const;

    //! Get reply-to address
    DLLLOCAL QoreValue getReplyTo(ExceptionSink* xsink) const;

    //! Get subject
    DLLLOCAL QoreValue getSubject(ExceptionSink* xsink) const;

    //! Get a const reference to the underlying proton::message
    DLLLOCAL const proton::message& getProtonMessage() const { return msg; }

    //! Get a mutable reference to the underlying proton::message
    DLLLOCAL proton::message& getProtonMessage() { return msg; }

private:
    proton::message msg;

    //! Store the original Qore body for exact round-trip
    QoreValue qore_body;

    //! Apply properties hash to the proton message
    DLLLOCAL void applyProperties(const QoreHashNode* properties, ExceptionSink* xsink);
};

#endif // _QORE_AMQP_MESSAGE_H
