# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpAddressDataProvider.qc Address-level data provider for AMQP

/*  AmqpAddressDataProvider.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! Address-level data provider for sending AMQP messages
/** Supports sending messages to a specific AMQP address via \c doRequest().

    The request hash accepts:
    - \c body: the message body (any type)
    - \c content_type: optional content type
    - \c message_id: optional message ID
    - \c correlation_id: optional correlation ID
    - \c reply_to: optional reply-to address
    - \c subject: optional subject
    - \c application_properties: optional application properties hash

    The response is a hash with delivery information.

    @par Example
    @code{.py}
auto result = address_provider.doRequest({"body": "Hello, AMQP!"});
    @endcode

*/
public class AmqpAddressDataProvider inherits AbstractDataProvider {
    public {
        #! Provider info
        const ProviderInfo = <DataProviderInfo>{
            "type": "AmqpAddressDataProvider",
            "supports_request": True,
            "supports_children": True,
            "children_can_support_observers": True,
        };

        #! Static request type for action registration
        static AmqpSendRequestDataType RequestType();

        #! Static response type for action registration
        static AmqpSendResponseDataType ResponseType();
    }

    private {
        #! The AMQP client
        Amqp::AmqpClient m_client;

        #! The address name
        string m_address;

        #! Default timeout
        int m_timeout_ms;

        #! Request type
        *AbstractDataProviderType m_request_type;

        #! Response type
        *AbstractDataProviderType m_response_type;
    }

    #! Creates the address data provider
    /** @param client the AMQP client
        @param address the AMQP address
        @param timeout_ms default timeout
    */
    constructor(Amqp::AmqpClient client, string address, int timeout_ms) {
        m_client = client;
        m_address = address;
        m_timeout_ms = timeout_ms;
    }

    #! Returns the provider name
    string getName() {
        return m_address;
    }

    #! Returns the provider description
    *string getDesc() {
        return sprintf("AMQP address '%s'", m_address);
    }

    #! Returns the request type
    private *AbstractDataProviderType getRequestTypeImpl() {
        if (!m_request_type) {
            m_request_type = new AmqpSendRequestDataType();
        }
        return m_request_type;
    }

    #! Returns the response type
    private *AbstractDataProviderType getResponseTypeImpl() {
        if (!m_response_type) {
            m_response_type = new AmqpSendResponseDataType();
        }
        return m_response_type;
    }

    #! Sends a message to this address
    private auto doRequestImpl(auto req, *hash<auto> request_options) {
        hash<auto> request = req;

        # Build message properties
        hash<AmqpMessageProperties> props();
        if (request.content_type) {
            props.content_type = request.content_type;
        }
        if (request.message_id) {
            props.message_id = request.message_id;
        }
        if (request.correlation_id) {
            props.correlation_id = request.correlation_id;
        }
        if (request.reply_to) {
            props.reply_to = request.reply_to;
        }
        if (request.subject) {
            props.subject = request.subject;
        }
        if (request.application_properties) {
            props.application_properties = request.application_properties;
        }

        hash<AmqpDeliveryInfo> info = m_client.send(m_address, request.body, props);
        return {
            "settled": info.settled,
            "state": info.state,
        };
    }

    #! Returns child provider names
    private *list<string> getChildProviderNamesImpl() {
        return ("events",);
    }

    #! Returns a child provider by name
    private *AbstractDataProvider getChildProviderImpl(string name) {
        if (name == "events") {
            return new AmqpEventDataProvider(m_client, m_address, m_timeout_ms);
        }
        return NOTHING;
    }

    #! Returns static provider info
    private hash<DataProviderInfo> getStaticInfoImpl() {
        return ProviderInfo;
    }
}

} # namespace AmqpDataProvider
