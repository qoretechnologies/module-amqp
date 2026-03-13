# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpEventDataProvider.qc Observable event provider for AMQP messages

/*  AmqpEventDataProvider.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! Event name for incoming AMQP messages
public const EVENT_AMQP_MESSAGE = "amqp-message";

#! Observable event data provider for incoming AMQP messages
/** Receives messages from an AMQP address and notifies registered observers.

    @par Event Data
    Each event is a hash with:
    - \c body: the message body
    - \c content_type: content type (if set)
    - \c message_id: message ID (if set)
    - \c correlation_id: correlation ID (if set)
    - \c reply_to: reply-to address (if set)
    - \c subject: subject (if set)
    - \c application_properties: application properties hash (if any)

    @since AmqpDataProvider 1.0
*/
public class AmqpEventDataProvider inherits AbstractDataProvider, DelayedObservable {
    public {
        #! Provider info
        const ProviderInfo = <DataProviderInfo>{
            "type": "AmqpEventDataProvider",
            "supports_observable": True,
        };

        #! Event fields
        const EventFields = {
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

    private {
        #! The AMQP client
        Amqp::AmqpClient m_client;

        #! The address name
        string m_address;

        #! Default timeout
        int m_timeout_ms;

        #! Stop flag
        bool m_stop = False;
    }

    #! Creates the event data provider
    /** @param client the AMQP client
        @param address the AMQP address to receive from
        @param timeout_ms default timeout
    */
    constructor(Amqp::AmqpClient client, string address, int timeout_ms) {
        m_client = client;
        m_address = address;
        m_timeout_ms = timeout_ms;
    }

    #! Returns the provider name
    string getName() {
        return "events";
    }

    #! Returns the provider description
    *string getDesc() {
        return sprintf("Observable events for AMQP address '%s'", m_address);
    }

    #! Returns the event types supported
    *hash<string, hash<DataProviderMessageInfo>> getEventTypesImpl() {
        return {
            EVENT_AMQP_MESSAGE: <DataProviderMessageInfo>{
                "desc": "Fired when a message is received on the AMQP address",
                "type": new HashDataType("AmqpMessageEvent", EventFields),
            },
        };
    }

    #! Called when observers are ready to receive events
    observersReady() {
        # Start receiving in a background thread
        background receiveLoop();
    }

    #! Stops event delivery
    stopEvents() {
        m_stop = True;
    }

    #! Returns static provider info
    private hash<DataProviderInfo> getStaticInfoImpl() {
        return ProviderInfo;
    }

    #! Background receive loop
    private receiveLoop() {
        while (!m_stop) {
            try {
                *AmqpMessage msg = m_client.receive(m_address, m_timeout_ms);
                if (msg) {
                    hash<auto> event_data = {
                        "body": msg.getBody(),
                        "content_type": msg.getContentType(),
                        "message_id": msg.getMessageId(),
                        "correlation_id": msg.getCorrelationId(),
                        "reply_to": msg.getReplyTo(),
                        "subject": msg.getSubject(),
                        "application_properties": msg.getApplicationProperties(),
                    };
                    notifyObservers(EVENT_AMQP_MESSAGE, event_data);
                }
            } catch (hash<ExceptionInfo> ex) {
                if (m_stop) {
                    break;
                }
                # Log error but continue receiving
            }
        }
    }
}

} # namespace AmqpDataProvider
