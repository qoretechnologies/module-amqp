# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpConsumer.qc dedicated AMQP consumer for an address

/*  AmqpConsumer.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! Dedicated consumer for receiving messages from a specific AMQP address
/** Wraps a receiver link on an @ref Qore::Amqp::AmqpConnection "AmqpConnection".
    Supports durable subscriptions and delivery disposition.

    @par Example:
    @code{.py}
AmqpClient client("amqp://guest:guest\@localhost:5672");
client.connect();

AmqpConsumer consumer = client.createConsumer("my-queue");
while (*AmqpMessage msg = consumer.receive(5s)) {
    printf("Got: %y\n", msg.getBody());
}

client.close();
    @endcode

*/
public class AmqpConsumer {
    private {
        #! The underlying connection
        AmqpConnection m_conn;

        #! The receiver link name
        string m_receiver;

        #! Durable subscription name (if any)
        *string m_subscription_name;
    }

    #! Create a new consumer
    /** @param conn the AMQP connection
        @param receiver the receiver link name
        @param subscription_name optional durable subscription name

    */
    constructor(AmqpConnection conn, string receiver, *string subscription_name) {
        m_conn = conn;
        m_receiver = receiver;
        m_subscription_name = subscription_name;
    }

    #! Receive a message
    /** @param timeout the maximum time to wait

        @return the received message, or NOTHING on timeout

    */
    *AmqpMessage receive(timeout timeout) {
        return m_conn.receive(m_receiver, timeout);
    }

    #! Accept a delivered message
    /** @param delivery_tag the delivery tag

    */
    accept(binary delivery_tag) {
        m_conn.accept(delivery_tag);
    }

    #! Reject a delivered message
    /** @param delivery_tag the delivery tag

    */
    reject(binary delivery_tag) {
        m_conn.reject(delivery_tag);
    }

    #! Release a delivered message for redelivery
    /** @param delivery_tag the delivery tag

    */
    release(binary delivery_tag) {
        m_conn.release(delivery_tag);
    }

    #! Modify a delivered message
    /** @param delivery_tag the delivery tag
        @param failed if True, the message was not processed successfully
        @param undeliverable if True, do not redeliver to this consumer
        @param annotations optional annotations to add

    */
    modify(binary delivery_tag, bool failed, bool undeliverable,
            *hash<auto> annotations) {
        m_conn.modify(delivery_tag, failed, undeliverable, annotations);
    }

    #! Close the consumer's receiver link
    /**
    */
    close() {
        m_conn.closeReceiver(m_receiver);
    }

    #! Close a durable receiver without unsubscribing
    /** The subscription persists on the broker and continues to accumulate messages.

        @throw AMQP-CONSUMER-ERROR if this is not a durable consumer

    */
    closeDurable() {
        if (!m_subscription_name) {
            throw "AMQP-CONSUMER-ERROR", "this consumer does not have a durable subscription";
        }
        m_conn.closeDurableReceiver(m_receiver);
    }

    #! Permanently remove the durable subscription from the broker
    /** @throw AMQP-CONSUMER-ERROR if this is not a durable consumer

    */
    unsubscribe() {
        if (!m_subscription_name) {
            throw "AMQP-CONSUMER-ERROR", "this consumer does not have a durable subscription";
        }
        m_conn.unsubscribeDurable(m_subscription_name);
    }

    #! Check if this is a durable consumer
    /** @return True if this consumer has a durable subscription

    */
    bool isDurable() {
        return exists m_subscription_name;
    }

    #! Get the receiver link name
    /** @return the receiver link name

    */
    string getReceiverName() {
        return m_receiver;
    }

    #! Get the durable subscription name
    /** @return the subscription name, or NOTHING if not durable

    */
    *string getSubscriptionName() {
        return m_subscription_name;
    }
}

} # namespace Amqp
