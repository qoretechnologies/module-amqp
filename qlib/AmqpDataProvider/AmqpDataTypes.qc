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

#! Common allowed content type values
public const ContentTypeAllowedValues = (
    <AllowedValueInfo>{
        "display_name": "Plain Text",
        "short_desc": "UTF-8 plain text",
        "desc": "Plain text message body",
        "value": "text/plain",
    },
    <AllowedValueInfo>{
        "display_name": "JSON",
        "short_desc": "JSON-encoded data",
        "desc": "Message body encoded as JSON (JavaScript Object Notation)",
        "value": "application/json",
    },
    <AllowedValueInfo>{
        "display_name": "XML",
        "short_desc": "XML-encoded data",
        "desc": "Message body encoded as XML",
        "value": "application/xml",
    },
    <AllowedValueInfo>{
        "display_name": "YAML",
        "short_desc": "YAML-encoded data",
        "desc": "Message body encoded as YAML",
        "value": "application/yaml",
    },
    <AllowedValueInfo>{
        "display_name": "Binary",
        "short_desc": "Raw binary data",
        "desc": "Raw binary message body (e.g., images, serialized objects)",
        "value": "application/octet-stream",
    },
);

#! Allowed delivery state values
public const DeliveryStateAllowedValues = (
    <AllowedValueInfo>{
        "display_name": "Accepted",
        "short_desc": "Message was accepted by the broker",
        "desc": "The broker accepted the message and it will be delivered to consumers",
        "value": "accepted",
    },
    <AllowedValueInfo>{
        "display_name": "Rejected",
        "short_desc": "Message was rejected by the broker",
        "desc": "The broker rejected the message; it may be routed to a dead letter queue",
        "value": "rejected",
    },
    <AllowedValueInfo>{
        "display_name": "Released",
        "short_desc": "Message was released for redelivery",
        "desc": "The message was released and will be redelivered to another consumer",
        "value": "released",
    },
    <AllowedValueInfo>{
        "display_name": "Modified",
        "short_desc": "Message was modified for redelivery",
        "desc": "The message was modified (e.g., marked as failed) and will be redelivered",
        "value": "modified",
    },
);

#! Request data type for sending AMQP messages
public class AmqpSendRequestDataType inherits DataProvider::HashDataType {
    public {
        #! Field definitions
        const Fields = {
            "body": {
                "display_name": "Message Body",
                "short_desc": "The message payload to send",
                "desc": "The message body to send to the AMQP address.\n\n"
                    "Can be any serializable type:\n"
                    "- **String**: plain text or encoded data (JSON, XML, YAML)\n"
                    "- **Hash**: structured data (automatically encoded as AMQP map)\n"
                    "- **List**: ordered collection\n"
                    "- **Binary**: raw binary data\n"
                    "- **Integer, float, boolean, date**: scalar values",
                "type": AbstractDataProviderTypeMap."auto",
                "required": True,
                "example_value": "Hello, World!",
            },
            "content_type": {
                "display_name": "Content Type",
                "short_desc": "MIME type of the message body",
                "desc": "The MIME content type describing the format of the message body.\n\n"
                    "Setting this helps consumers know how to interpret the message. "
                    "Common values include `text/plain` for simple text, "
                    "`application/json` for structured data, and "
                    "`application/octet-stream` for binary payloads.",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "application/json",
                "allowed_values": ContentTypeAllowedValues,
            },
            "message_id": {
                "display_name": "Message ID",
                "short_desc": "Unique identifier for the message",
                "desc": "A unique identifier for this message, used for deduplication "
                    "and tracking.\n\n"
                    "If not provided, the broker may assign one automatically. "
                    "Use a UUID or application-specific format for reliable tracking.",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "order-2026-03-28-001",
            },
            "correlation_id": {
                "display_name": "Correlation ID",
                "short_desc": "Links related messages together",
                "desc": "An identifier that links this message to a related message, "
                    "typically used in request-reply patterns.\n\n"
                    "Set this to the `message_id` of the original request when "
                    "sending a reply, so the requester can match the response.",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "req-12345",
            },
            "reply_to": {
                "display_name": "Reply-To Address",
                "short_desc": "Address for reply messages",
                "desc": "The AMQP address where replies to this message should be sent.\n\n"
                    "Used in request-reply patterns to tell the receiver where to "
                    "send the response.",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "reply-queue",
            },
            "subject": {
                "display_name": "Subject",
                "short_desc": "Message subject or routing key",
                "desc": "The subject of the message, which can be used for "
                    "content-based routing or categorization.\n\n"
                    "Some brokers use this as a routing key to direct messages "
                    "to specific queues or consumers.",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "orders.new",
            },
            "application_properties": {
                "display_name": "Application Properties",
                "short_desc": "Custom key-value metadata",
                "desc": "A hash of custom key-value properties attached to the message.\n\n"
                    "Use application properties to add business-specific metadata "
                    "such as customer IDs, priority levels, or processing flags. "
                    "These properties can be used by consumers for filtering and routing.",
                "type": AbstractDataProviderTypeMap."*hash",
                "example_value": {"priority": "high", "customer_id": "C-1234"},
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
                "short_desc": "Whether the broker confirmed the message",
                "desc": "Indicates whether the broker has settled (confirmed) the message.\n\n"
                    "`True` means the broker accepted responsibility for the message "
                    "and it will be delivered to consumers. `False` may indicate the "
                    "message is still in-flight or was not confirmed.",
                "type": AbstractDataProviderTypeMap."bool",
                "example_value": True,
            },
            "state": {
                "display_name": "Delivery State",
                "short_desc": "Broker's disposition of the message",
                "desc": "The delivery outcome reported by the broker:\n"
                    "- `accepted`: message was accepted and will be delivered\n"
                    "- `rejected`: message was rejected (may go to dead letter queue)\n"
                    "- `released`: message was released for redelivery\n"
                    "- `modified`: message was modified and will be redelivered",
                "type": AbstractDataProviderTypeMap."string",
                "example_value": "accepted",
                "allowed_values": DeliveryStateAllowedValues,
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
                "display_name": "Message Body",
                "short_desc": "The received message payload",
                "desc": "The body of the received message.\n\n"
                    "The type depends on what the sender sent — it may be a string, "
                    "hash, list, binary, or scalar value. Check `content_type` to "
                    "determine how to interpret the body.",
                "type": AbstractDataProviderTypeMap."auto",
                "example_value": "Hello, AMQP!",
            },
            "content_type": {
                "display_name": "Content Type",
                "short_desc": "MIME type of the message body",
                "desc": "The MIME content type of the received message body, if set by "
                    "the sender.\n\n"
                    "Use this to determine how to parse the body (e.g., "
                    "`application/json` → parse as JSON).",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "text/plain",
                "allowed_values": ContentTypeAllowedValues,
            },
            "message_id": {
                "display_name": "Message ID",
                "short_desc": "Unique identifier of the message",
                "desc": "The unique identifier assigned to this message by the sender, "
                    "useful for deduplication and tracking.",
                "type": AbstractDataProviderTypeMap."auto",
                "example_value": "msg-001",
            },
            "correlation_id": {
                "display_name": "Correlation ID",
                "short_desc": "Links this message to a related request",
                "desc": "The correlation identifier linking this message to a previous "
                    "request, used in request-reply patterns.",
                "type": AbstractDataProviderTypeMap."auto",
                "example_value": "req-12345",
            },
            "reply_to": {
                "display_name": "Reply-To Address",
                "short_desc": "Address for sending replies",
                "desc": "The address where the sender expects replies to be sent.",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "reply-queue",
            },
            "subject": {
                "display_name": "Subject",
                "short_desc": "Message subject or routing key",
                "desc": "The subject of the message, which may indicate the message "
                    "category or routing key.",
                "type": AbstractDataProviderTypeMap."*string",
                "example_value": "orders.new",
            },
            "application_properties": {
                "display_name": "Application Properties",
                "short_desc": "Custom key-value metadata from the sender",
                "desc": "Custom application-defined properties attached to the message "
                    "by the sender.\n\n"
                    "These may contain business-specific metadata such as priority, "
                    "customer IDs, or processing instructions.",
                "type": AbstractDataProviderTypeMap."*hash",
                "example_value": {"source": "order-service"},
            },
        };
    }

    #! Creates the type
    constructor() : HashDataType("AmqpMessageEvent") {
        addQoreFields(Fields);
    }
}

} # namespace AmqpDataProvider
