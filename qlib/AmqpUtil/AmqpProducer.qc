# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpProducer.qc dedicated AMQP producer for an address

/*  AmqpProducer.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! Dedicated producer for sending messages to a specific AMQP address
/** Wraps a sender link on an @ref Qore::Amqp::AmqpConnection "AmqpConnection".

    @par Example:
    @code{.py}
AmqpClient client("amqp://guest:guest\@localhost:5672");
client.connect();

AmqpProducer producer = client.createProducer("my-queue");
producer.send("Hello!");
producer.send({"key": "value"}, <AmqpMessageProperties>{"content_type": "application/json"});

client.close();
    @endcode

*/
public class AmqpProducer {
    private {
        #! The underlying connection
        AmqpConnection m_conn;

        #! The sender link name
        string m_sender;

        #! Default send options
        *hash<AmqpSendOptions> m_opts;
    }

    #! Create a new producer
    /** @param conn the AMQP connection
        @param sender the sender link name
        @param opts default send options applied to all messages

    */
    constructor(AmqpConnection conn, string sender, *hash<AmqpSendOptions> opts) {
        m_conn = conn;
        m_sender = sender;
        m_opts = opts;
    }

    #! Send a message body with optional properties
    /** @param body the message body
        @param properties optional message properties
        @param opts optional send options (overrides default opts)

        @return delivery information

    */
    hash<AmqpDeliveryInfo> send(auto body, *hash<AmqpMessageProperties> properties,
            *hash<AmqpSendOptions> opts) {
        return m_conn.send(m_sender, new AmqpMessage(body, properties), opts ?? m_opts);
    }

    #! Send an AmqpMessage
    /** @param msg the message to send
        @param opts optional send options (overrides default opts)

        @return delivery information

    */
    hash<AmqpDeliveryInfo> sendMessage(AmqpMessage msg, *hash<AmqpSendOptions> opts) {
        return m_conn.send(m_sender, msg, opts ?? m_opts);
    }

    #! Close the producer's sender link
    /**
    */
    close() {
        m_conn.closeSender(m_sender);
    }

    #! Get the sender link name
    /** @return the sender link name

    */
    string getSenderName() {
        return m_sender;
    }
}

} # namespace Amqp
