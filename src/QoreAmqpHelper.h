/* -*- mode: c++; indent-tabs-mode: nil -*- */
/** @file QoreAmqpHelper.h AMQP utility functions */
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

#ifndef _QORE_AMQP_HELPER_H
#define _QORE_AMQP_HELPER_H

#include "amqp-module.h"

#include <proton/types.hpp>
#include <proton/value.hpp>
#include <proton/message.hpp>
#include <proton/scalar.hpp>
#include <proton/scalar_base.hpp>
#include <proton/message_id.hpp>
#include <proton/binary.hpp>
#include <proton/timestamp.hpp>
#include <proton/uuid.hpp>
#include <proton/map.hpp>

#include <string>

//! Static utility class for AMQP type conversions
class QoreAmqpHelper {
public:
    //! Convert a proton::value to a QoreValue
    DLLLOCAL static QoreValue protonToQore(const proton::value& val, ExceptionSink* xsink);

    //! Convert a QoreValue to a proton::value
    DLLLOCAL static proton::value qoreToProton(const QoreValue& val, ExceptionSink* xsink);

    //! Convert a proton::scalar to a QoreValue
    DLLLOCAL static QoreValue scalarToQore(const proton::scalar& sc, ExceptionSink* xsink);

    //! Convert a proton::scalar_base (message_id, annotation_key) to a QoreValue
    DLLLOCAL static QoreValue scalarBaseToQore(const proton::scalar_base& sb, ExceptionSink* xsink);

    //! Convert a QoreValue to a proton::scalar
    DLLLOCAL static proton::scalar qoreToScalar(const QoreValue& val, ExceptionSink* xsink);

    //! Convert a QoreValue to a proton::message_id (string, uint64_t, binary, or uuid)
    DLLLOCAL static proton::message_id qoreToMessageId(const QoreValue& val, ExceptionSink* xsink);

    //! Convert a proton map (string keys) to a QoreHashNode
    DLLLOCAL static QoreHashNode* protonMapToHash(const proton::value& val, ExceptionSink* xsink);

    //! Convert a QoreHashNode to a proton map value
    DLLLOCAL static proton::value hashToProtonMap(const QoreHashNode* hash, ExceptionSink* xsink);

    //! Convert a proton list to a QoreListNode
    DLLLOCAL static QoreListNode* protonListToQore(const proton::value& val, ExceptionSink* xsink);

    //! Convert a QoreListNode to a proton list value
    DLLLOCAL static proton::value listToProton(const QoreListNode* list, ExceptionSink* xsink);

    //! Convert a proton::timestamp to a DateTimeNode
    DLLLOCAL static DateTimeNode* timestampToDate(proton::timestamp ts);

    //! Convert a DateTimeNode to a proton::timestamp
    DLLLOCAL static proton::timestamp dateToTimestamp(const DateTimeNode* dt);

    //! Convert a proton::binary to a BinaryNode
    DLLLOCAL static BinaryNode* protonBinaryToQore(const proton::binary& bin);

    //! Convert a BinaryNode to a proton::binary
    DLLLOCAL static proton::binary qoreBinaryToProton(const BinaryNode* bin);

    //! Convert a proton::uuid to a string
    DLLLOCAL static QoreStringNode* uuidToString(const proton::uuid& uuid);

    //! Convert a proton::message::property_map to a QoreHashNode
    DLLLOCAL static QoreHashNode* propertyMapToHash(const proton::message::property_map& pm,
        ExceptionSink* xsink);

    //! Convert a proton::message::annotation_map to a QoreHashNode
    DLLLOCAL static QoreHashNode* annotationMapToHash(const proton::message::annotation_map& am,
        ExceptionSink* xsink);

    //! Format a proton error message for Qore exceptions
    DLLLOCAL static std::string formatError(const std::string& prefix, const std::string& msg);

    //! Parse an AMQP URL to extract hostname and port
    /** @param url the AMQP URL (amqp://... or amqps://...)
        @param host output: the hostname
        @param port output: the port number (defaults to 5672 for amqp, 5671 for amqps)
    */
    DLLLOCAL static void parseUrlHostPort(const std::string& url, std::string& host, int& port);

private:
    QoreAmqpHelper() = delete;
};

#endif // _QORE_AMQP_HELPER_H
