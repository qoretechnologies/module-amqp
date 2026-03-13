# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpRequestReply.qc AMQP request-reply pattern helper

/*  AmqpRequestReply.qc Copyright 2026 Qore Technologies, s.r.o.

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

public namespace Amqp {

#! Request-reply pattern helper for AMQP
/** Sends request messages with auto-generated message IDs and reply-to addresses,
    then waits for correlated responses.

    @par Example:
    @code{.py}
AmqpClient client("amqp://guest:guest\@localhost:5672");
client.connect();

AmqpRequestReply rr(client, "request-queue");
AmqpMessage response = rr.request("ping", 10s);
printf("Reply: %y\n", response.getBody());

rr.close();
client.close();
    @endcode

    If no \c reply_address is provided, a unique dynamic reply address is generated
    using the format \c "reply-<pid>-<counter>".

    @since AmqpUtil 1.0
*/
public class AmqpRequestReply {
    private {
        #! The underlying connection
        AmqpConnection m_conn;

        #! Sender link name for requests
        string m_sender;

        #! Receiver link name for replies
        string m_receiver;

        #! The reply-to address
        string m_reply_address;

        #! Counter for generating unique message IDs
        int m_counter = 0;
    }

    #! Create a request-reply helper
    /** @param client the AMQP client
        @param request_address the address to send requests to
        @param reply_address optional address for replies (auto-generated if omitted)

        @since AmqpUtil 1.0
    */
    constructor(AmqpClient client, string request_address, *string reply_address) {
        m_conn = client.getConnection();
        m_sender = m_conn.createSender(request_address);

        if (reply_address) {
            m_reply_address = reply_address;
        } else {
            m_reply_address = sprintf("reply-%d-%d", getpid(), gettid());
        }
        m_receiver = m_conn.createReceiver(m_reply_address);
    }

    #! Send a request and wait for a correlated response
    /** @param body the request body
        @param timeout the maximum time to wait for a response
        @param properties optional additional message properties

        @return the response message

        @throw AMQP-REQUEST-REPLY-TIMEOUT if no response is received within the timeout

        @since AmqpUtil 1.0
    */
    AmqpMessage request(auto body, timeout timeout, *hash<AmqpMessageProperties> properties) {
        # Generate a unique message ID
        string msg_id = sprintf("req-%d-%d-%d", getpid(), gettid(), ++m_counter);

        # Build properties with reply_to and message_id
        hash<AmqpMessageProperties> props = properties ?? <AmqpMessageProperties>{};
        props.message_id = msg_id;
        props.reply_to = m_reply_address;

        # Send the request
        m_conn.send(m_sender, new AmqpMessage(body, props));

        # Wait for a correlated response
        date deadline = now_us() + milliseconds(timeout);
        while (True) {
            timeout remaining = deadline - now_us();
            if (remaining <= 0ms) {
                throw "AMQP-REQUEST-REPLY-TIMEOUT",
                    sprintf("no response received for request '%s' within timeout", msg_id);
            }

            *AmqpMessage response = m_conn.receive(m_receiver, remaining);
            if (!response) {
                throw "AMQP-REQUEST-REPLY-TIMEOUT",
                    sprintf("no response received for request '%s' within timeout", msg_id);
            }

            # Check correlation ID matches our message ID
            auto corr_id = response.getCorrelationId();
            if (corr_id == msg_id) {
                return response;
            }

            # Not our response — discard and try again
        }
    }

    #! Close the request-reply helper (closes sender and receiver)
    /** @since AmqpUtil 1.0
    */
    close() {
        # Links are cleaned up when the connection closes
    }
}

} # namespace Amqp
