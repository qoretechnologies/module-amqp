/* -*- mode: c++; indent-tabs-mode: nil -*- */
/** @file amqp-module.cpp amqp module implementation */
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

#include "amqp-module.h"
#include "QC_AmqpConnection.h"
#include "QC_AmqpMessage.h"

static void amqp_module_init(QoreModuleInitContext& ctx, ExceptionSink& xsink);
static void amqp_module_ns_init(QoreNamespace* rns, QoreNamespace* qns, ExceptionSink& xsink);
static void amqp_module_delete();

extern "C" DLLEXPORT void amqp_qore_module_desc(QoreModuleInfo& mod_info) {
    mod_info.name = "amqp";
    mod_info.version = "1.0.0";
    mod_info.desc = "Qore AMQP 1.0 module";
    mod_info.author = "Qore Technologies, s.r.o.";
    mod_info.url = "https://github.com/qoretechnologies/module-amqp";
    mod_info.api_major = QORE_MODULE_API_MAJOR;
    mod_info.api_minor = QORE_MODULE_API_MINOR;
    mod_info.init = amqp_module_init;
    mod_info.ns_init = amqp_module_ns_init;
    mod_info.del = amqp_module_delete;
    mod_info.license = QL_MIT;
    mod_info.license_str = "MIT";
}

// Global hashdecl pointers
const TypedHashDecl* hashdeclAmqpConnectionOptions = nullptr;
const TypedHashDecl* hashdeclAmqpSslOptions = nullptr;
const TypedHashDecl* hashdeclAmqpSaslOptions = nullptr;
const TypedHashDecl* hashdeclAmqpDeliveryInfo = nullptr;
const TypedHashDecl* hashdeclAmqpSendOptions = nullptr;
const TypedHashDecl* hashdeclAmqpReceiveOptions = nullptr;
const TypedHashDecl* hashdeclAmqpFilterOptions = nullptr;
const TypedHashDecl* hashdeclAmqpMessageProperties = nullptr;
const TypedHashDecl* hashdeclAmqpAddressInfo = nullptr;
const TypedHashDecl* hashdeclAmqpQueueInfo = nullptr;
const TypedHashDecl* hashdeclAmqpConnectionStats = nullptr;

QoreNamespace AmqpNs("Qore::Amqp");

#include <proton/sender.hpp>
#include <proton/receiver.hpp>
#include <proton/connection.hpp>
#include <proton/tracker.hpp>
#include <proton/message.hpp>
#include <proton/link.hpp>
#include <proton/internal/object.hpp>

static void amqp_module_init(QoreModuleInitContext& ctx, ExceptionSink& xsink) {
    // Debug: print Proton C++ type layout for cross-platform verification
    fprintf(stderr, "AMQP module Proton layout: object=%zu sender=%zu(poly=%d) "
        "receiver=%zu(poly=%d) connection=%zu(poly=%d) tracker=%zu(poly=%d) "
        "message=%zu(poly=%d) link=%zu(poly=%d)\n",
        sizeof(proton::internal::object<pn_link_t>),
        sizeof(proton::sender), std::is_polymorphic_v<proton::sender>,
        sizeof(proton::receiver), std::is_polymorphic_v<proton::receiver>,
        sizeof(proton::connection), std::is_polymorphic_v<proton::connection>,
        sizeof(proton::tracker), std::is_polymorphic_v<proton::tracker>,
        sizeof(proton::message), std::is_polymorphic_v<proton::message>,
        sizeof(proton::link), std::is_polymorphic_v<proton::link>);

    // Initialize hashdecls (dependency order — SSL/SASL before ConnectionOptions)
    hashdeclAmqpSslOptions = init_hashdecl_AmqpSslOptions(AmqpNs);
    hashdeclAmqpSaslOptions = init_hashdecl_AmqpSaslOptions(AmqpNs);
    hashdeclAmqpConnectionOptions = init_hashdecl_AmqpConnectionOptions(AmqpNs);
    hashdeclAmqpMessageProperties = init_hashdecl_AmqpMessageProperties(AmqpNs);
    hashdeclAmqpDeliveryInfo = init_hashdecl_AmqpDeliveryInfo(AmqpNs);
    hashdeclAmqpSendOptions = init_hashdecl_AmqpSendOptions(AmqpNs);
    hashdeclAmqpReceiveOptions = init_hashdecl_AmqpReceiveOptions(AmqpNs);
    hashdeclAmqpFilterOptions = init_hashdecl_AmqpFilterOptions(AmqpNs);
    hashdeclAmqpAddressInfo = init_hashdecl_AmqpAddressInfo(AmqpNs);
    hashdeclAmqpQueueInfo = init_hashdecl_AmqpQueueInfo(AmqpNs);
    hashdeclAmqpConnectionStats = init_hashdecl_AmqpConnectionStats(AmqpNs);

    // Initialize classes
    AmqpNs.addSystemClass(initAmqpMessageClass(AmqpNs));
    AmqpNs.addSystemClass(initAmqpConnectionClass(AmqpNs));
}

static void amqp_module_ns_init(QoreNamespace* rns, QoreNamespace* qns, ExceptionSink& xsink) {
    qns->addNamespace(AmqpNs.copy());
}

static void amqp_module_delete() {
    // No global cleanup needed
}
