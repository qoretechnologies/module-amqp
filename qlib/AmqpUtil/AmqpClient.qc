# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpClient.qc high-level AMQP client

/*  AmqpClient.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! High-level AMQP 1.0 client for connection management, sending, and receiving
/** Wraps @ref Qore::Amqp::AmqpConnection "AmqpConnection" with a convenient API for
    common messaging patterns.

    @par Example:
    @code{.py}
AmqpClient client("amqp://guest:guest\@localhost:5672");
client.connect();

client.send("my-queue", "Hello!");
*AmqpMessage msg = client.receive("my-queue", 5s);
if (msg) {
    printf("Got: %y\n", msg.getBody());
}

client.close();
    @endcode

    @since AmqpUtil 1.0
*/
public class AmqpClient {
    private {
        #! The underlying AMQP connection
        AmqpConnection m_conn;

        #! Cached sender link names by address
        hash<string, string> m_senders;

        #! Cached receiver link names by address
        hash<string, string> m_receivers;
    }

    #! Create a new AMQP client from a URL string
    /** @param url the broker URL (e.g., "amqp://guest:guest\@localhost:5672")
        @param options additional connection options (url key is overridden)

        @throw AMQP-CONNECTION-ERROR if the options are invalid

        @since AmqpUtil 1.0
    */
    constructor(string url, *hash<auto> options) {
        hash<AmqpConnectionOptions> opts = cast<hash<AmqpConnectionOptions>>(options ?? {});
        opts.url = url;
        m_conn = new AmqpConnection(opts);
    }

    #! Create a new AMQP client from connection options
    /** @param options connection options including the broker URL

        @throw AMQP-CONNECTION-ERROR if the options are invalid

        @since AmqpUtil 1.0
    */
    constructor(hash<AmqpConnectionOptions> options) {
        m_conn = new AmqpConnection(options);
    }

    #! Connect to the AMQP broker
    /** @throw AMQP-CONNECTION-ERROR if the connection fails

        @since AmqpUtil 1.0
    */
    connect() {
        m_conn.connect();
    }

    #! Close the connection
    /** @since AmqpUtil 1.0
    */
    close() {
        m_conn.close();
        m_senders = {};
        m_receivers = {};
    }

    #! Returns True if connected
    /** @return True if the connection is open

        @since AmqpUtil 1.0
    */
    bool isConnected() {
        return m_conn.isConnected();
    }

    #! Get the underlying AmqpConnection object
    /** @return the underlying connection

        @since AmqpUtil 1.0
    */
    AmqpConnection getConnection() {
        return m_conn;
    }

    #! Send a message to an address
    /** Creates a sender link on first use for each address (cached for reuse).

        @param address the AMQP address to send to
        @param body the message body (any supported type)
        @param properties optional message properties
        @param opts optional send options

        @return delivery information

        @throw AMQP-CONNECTION-ERROR if not connected
        @throw AMQP-SEND-ERROR if the send fails

        @since AmqpUtil 1.0
    */
    hash<AmqpDeliveryInfo> send(string address, auto body,
            *hash<AmqpMessageProperties> properties, *hash<AmqpSendOptions> opts) {
        string sender = getOrCreateSender(address);
        return m_conn.send(sender, new AmqpMessage(body, properties), opts);
    }

    #! Send an AmqpMessage to an address
    /** @param address the AMQP address to send to
        @param msg the message to send
        @param opts optional send options

        @return delivery information

        @since AmqpUtil 1.0
    */
    hash<AmqpDeliveryInfo> sendMessage(string address, AmqpMessage msg,
            *hash<AmqpSendOptions> opts) {
        string sender = getOrCreateSender(address);
        return m_conn.send(sender, msg, opts);
    }

    #! Receive a message from an address
    /** Creates a receiver link on first use for each address (cached for reuse).

        @param address the AMQP address to receive from
        @param timeout the maximum time to wait

        @return the received message, or NOTHING on timeout

        @throw AMQP-CONNECTION-ERROR if not connected

        @since AmqpUtil 1.0
    */
    *AmqpMessage receive(string address, timeout timeout) {
        string receiver = getOrCreateReceiver(address);
        return m_conn.receive(receiver, timeout);
    }

    #! Accept a delivered message
    /** @param delivery_tag the delivery tag from the message

        @since AmqpUtil 1.0
    */
    accept(binary delivery_tag) {
        m_conn.accept(delivery_tag);
    }

    #! Reject a delivered message
    /** @param delivery_tag the delivery tag from the message

        @since AmqpUtil 1.0
    */
    reject(binary delivery_tag) {
        m_conn.reject(delivery_tag);
    }

    #! Release a delivered message for redelivery
    /** @param delivery_tag the delivery tag from the message

        @since AmqpUtil 1.0
    */
    release(binary delivery_tag) {
        m_conn.release(delivery_tag);
    }

    #! Modify a delivered message
    /** @param delivery_tag the delivery tag
        @param failed if True, the message was not processed successfully
        @param undeliverable if True, do not redeliver to this consumer
        @param annotations optional annotations to add

        @since AmqpUtil 1.0
    */
    modify(binary delivery_tag, bool failed, bool undeliverable,
            *hash<auto> annotations) {
        m_conn.modify(delivery_tag, failed, undeliverable, annotations);
    }

    #! Create a producer for a specific address
    /** @param address the AMQP address to produce to
        @param opts optional send options applied to all messages

        @return a new AmqpProducer

        @since AmqpUtil 1.0
    */
    AmqpProducer createProducer(string address, *hash<AmqpSendOptions> opts) {
        string sender = getOrCreateSender(address);
        return new AmqpProducer(m_conn, sender, opts);
    }

    #! Create a consumer for a specific address
    /** @param address the AMQP address to consume from
        @param opts optional receiver options
        @param filter optional filter options

        @return a new AmqpConsumer

        @since AmqpUtil 1.0
    */
    AmqpConsumer createConsumer(string address, *hash<AmqpReceiveOptions> opts,
            *hash<AmqpFilterOptions> filter) {
        string receiver;
        if (filter) {
            # Filtered receivers need their own links (can't reuse cached)
            receiver = m_conn.createDurableReceiver(address, "", opts, filter);
        } else {
            receiver = getOrCreateReceiver(address, opts);
        }
        return new AmqpConsumer(m_conn, receiver);
    }

    #! Create a durable consumer for a specific address
    /** @param address the AMQP address to consume from
        @param subscription_name the durable subscription name
        @param opts optional receiver options
        @param filter optional filter options

        @return a new AmqpConsumer with durable subscription

        @since AmqpUtil 1.0
    */
    AmqpConsumer createDurableConsumer(string address, string subscription_name,
            *hash<AmqpReceiveOptions> opts, *hash<AmqpFilterOptions> filter) {
        string receiver = m_conn.createDurableReceiver(address, subscription_name,
            opts, filter);
        return new AmqpConsumer(m_conn, receiver, subscription_name);
    }

    #! Get a management client for broker introspection
    /** @return a new AmqpManagementClient

        @since AmqpUtil 1.0
    */
    AmqpManagementClient getManagementClient() {
        return new AmqpManagementClient(m_conn);
    }

    #! Begin a transaction
    /** @throw AMQP-TRANSACTION-ERROR if a transaction is already active

        @since AmqpUtil 1.0
    */
    beginTransaction() {
        m_conn.beginTransaction();
    }

    #! Commit the current transaction
    /** @throw AMQP-TRANSACTION-ERROR if no transaction is active

        @since AmqpUtil 1.0
    */
    commitTransaction() {
        m_conn.commitTransaction();
    }

    #! Rollback the current transaction
    /** @throw AMQP-TRANSACTION-ERROR if no transaction is active

        @since AmqpUtil 1.0
    */
    rollbackTransaction() {
        m_conn.rollbackTransaction();
    }

    #! Check if a transaction is active
    /** @return True if a transaction is active

        @since AmqpUtil 1.0
    */
    bool inTransaction() {
        return m_conn.inTransaction();
    }

    #! Get or create a sender for an address
    private string getOrCreateSender(string address) {
        *string sender = m_senders{address};
        if (!sender) {
            sender = m_conn.createSender(address);
            m_senders{address} = sender;
        }
        return sender;
    }

    #! Get or create a receiver for an address
    private string getOrCreateReceiver(string address, *hash<AmqpReceiveOptions> opts) {
        *string receiver = m_receivers{address};
        if (!receiver) {
            receiver = m_conn.createReceiver(address, opts);
            m_receivers{address} = receiver;
        }
        return receiver;
    }
}

} # namespace Amqp
