# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpDataTypes.qc Request and response data types for AMQP data providers

/*  AmqpDataTypes.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! Contains all public definitions in the AmqpDataProvider module
public namespace AmqpDataProvider {

#! Request data type for sending AMQP messages
public class AmqpSendRequestDataType inherits DataProvider::HashDataType {
    public {
        #! Field definitions
        const Fields = {
            "body": {
                "display_name": "Body",
                "short_desc": "The message body",
                "desc": "The message body to send (string, binary, hash, list, int, etc.)",
                "type": AbstractDataProviderTypeMap."auto",
                "required": True,
            },
            "content_type": {
                "display_name": "Content Type",
                "short_desc": "Message content type",
                "desc": "The MIME content type of the message body "
                    "(e.g., `text/plain`, `application/json`)",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "text/plain",
            },
            "message_id": {
                "display_name": "Message ID",
                "short_desc": "Message identifier",
                "desc": "A unique identifier for the message",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "msg-001",
            },
            "correlation_id": {
                "display_name": "Correlation ID",
                "short_desc": "Correlation identifier",
                "desc": "An identifier used to correlate request/response messages",
                "type": AbstractDataProviderTypeMap."*string",
            },
            "reply_to": {
                "display_name": "Reply To",
                "short_desc": "Reply-to address",
                "desc": "The address to which replies should be sent",
                "type": AbstractDataProviderTypeMap."*string",
            },
            "subject": {
                "display_name": "Subject",
                "short_desc": "Message subject",
                "desc": "The subject of the message",
                "type": AbstractDataProviderTypeMap."*string",
            },
            "application_properties": {
                "display_name": "Application Properties",
                "short_desc": "Application-defined properties",
                "desc": "A hash of application-defined key-value properties sent with "
                    "the message",
                "type": AbstractDataProviderTypeMap."*hash",
            },
        };
    }

    #! Creates the type
    constructor() : HashDataType("AmqpSendRequest") {
        addQoreFields(Fields);
    }
}

#! Response data type for AMQP send operations
public class AmqpSendResponseDataType inherits DataProvider::HashDataType {
    public {
        #! Field definitions
        const Fields = {
            "settled": {
                "display_name": "Settled",
                "short_desc": "Whether the delivery was settled",
                "desc": "`True` if the delivery was settled by the broker",
                "type": AbstractDataProviderTypeMap."bool",
            },
            "state": {
                "display_name": "State",
                "short_desc": "Delivery state",
                "desc": "The delivery state (e.g., `accepted`)",
                "type": AbstractDataProviderTypeMap."string",
            },
        };
    }

    #! Creates the type
    constructor() : HashDataType("AmqpSendResponse") {
        addQoreFields(Fields);
    }
}

#! Event data type for incoming AMQP messages
public class AmqpMessageEventDataType inherits DataProvider::HashDataType {
    public {
        #! Field definitions
        const Fields = {
            "body": {
                "display_name": "Body",
                "short_desc": "The message body",
                "desc": "The received message body",
                "type": AbstractDataProviderTypeMap."auto",
            },
            "content_type": {
                "display_name": "Content Type",
                "short_desc": "Message content type",
                "desc": "The MIME content type of the message",
                "type": AbstractDataProviderTypeMap."*string",
            },
            "message_id": {
                "display_name": "Message ID",
                "short_desc": "Message identifier",
                "desc": "The message identifier",
                "type": AbstractDataProviderTypeMap."auto",
            },
            "correlation_id": {
                "display_name": "Correlation ID",
                "short_desc": "Correlation identifier",
                "desc": "The correlation identifier",
                "type": AbstractDataProviderTypeMap."auto",
            },
            "reply_to": {
                "display_name": "Reply To",
                "short_desc": "Reply-to address",
                "desc": "The reply-to address",
                "type": AbstractDataProviderTypeMap."*string",
            },
            "subject": {
                "display_name": "Subject",
                "short_desc": "Message subject",
                "desc": "The message subject",
                "type": AbstractDataProviderTypeMap."*string",
            },
            "application_properties": {
                "display_name": "Application Properties",
                "short_desc": "Application-defined properties",
                "desc": "Application-defined message properties",
                "type": AbstractDataProviderTypeMap."*hash",
            },
        };
    }

    #! Creates the type
    constructor() : HashDataType("AmqpMessageEvent") {
        addQoreFields(Fields);
    }
}

} # namespace AmqpDataProvider
