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

    */
    constructor(string url, *hash<auto> options) {
        hash<AmqpConnectionOptions> opts = cast<hash<AmqpConnectionOptions>>(options ?? {});
        opts.url = url;
        m_conn = new AmqpConnection(opts);
    }

    #! Create a new AMQP client from connection options
    /** @param options connection options including the broker URL

        @throw AMQP-CONNECTION-ERROR if the options are invalid

    */
    constructor(hash<AmqpConnectionOptions> options) {
        m_conn = new AmqpConnection(options);
    }

    #! Connect to the AMQP broker
    /** @throw AMQP-CONNECTION-ERROR if the connection fails

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

    */
    bool isConnected() {
        return m_conn.isConnected();
    }

    #! Close a sender link by address
    /** @param address the address whose sender to close

    */
    closeSender(string address) {
        *string sender = remove m_senders{address};
        if (sender) {
            m_conn.closeSender(sender);
        }
    }

    #! Close a receiver link by address
    /** @param address the address whose receiver to close

    */
    closeReceiver(string address) {
        *string receiver = remove m_receivers{address};
        if (receiver) {
            m_conn.closeReceiver(receiver);
        }
    }

    #! Returns connection statistics
    /** @return connection statistics hash

    */
    hash<AmqpConnectionStats> getStatistics() {
        return m_conn.getStatistics();
    }

    #! Returns and drains pending connection lifecycle events
    /** @return list of event hashes
    */
    list<hash<auto>> getConnectionEvents() {
        return m_conn.getConnectionEvents();
    }

    #! Sets whether links are automatically recovered on reconnect
    /** @param recover True to enable, False to disable (default: True)
    */
    setAutoRecoverLinks(bool recover) {
        m_conn.setAutoRecoverLinks(recover);
    }

    #! Get the underlying AmqpConnection object
    /** @return the underlying connection

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

    */
    *AmqpMessage receive(string address, timeout timeout) {
        string receiver = getOrCreateReceiver(address);
        return m_conn.receive(receiver, timeout);
    }

    #! Accept a delivered message
    /** @param delivery_tag the delivery tag from the message

    */
    accept(binary delivery_tag) {
        m_conn.accept(delivery_tag);
    }

    #! Reject a delivered message
    /** @param delivery_tag the delivery tag from the message

    */
    reject(binary delivery_tag) {
        m_conn.reject(delivery_tag);
    }

    #! Release a delivered message for redelivery
    /** @param delivery_tag the delivery tag from the message

    */
    release(binary delivery_tag) {
        m_conn.release(delivery_tag);
    }

    #! Send a batch of message bodies to an address
    /** @param address the AMQP address
        @param bodies list of message bodies to send

        @return list of delivery info hashes
    */
    list<hash<AmqpDeliveryInfo>> sendBatch(string address, list<auto> bodies) {
        string sender = getOrCreateSender(address);
        list<AmqpMessage> msgs = map new AmqpMessage($1), bodies;
        return m_conn.sendBatch(sender, msgs);
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

    #! Create a producer for a specific address
    /** @param address the AMQP address to produce to
        @param opts optional send options applied to all messages

        @return a new AmqpProducer

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

    #! Create a shared consumer for a specific address
    /** Multiple consumers with the same subscription name share the message load.

        @param address the AMQP address to consume from
        @param subscription_name the shared subscription name
        @param opts optional receiver options (shared is set automatically)
        @param global if True, the subscription is shared across connections

        @return a new AmqpConsumer with shared subscription
    */
    AmqpConsumer createSharedConsumer(string address, string subscription_name,
            *hash<AmqpReceiveOptions> opts, bool global = False) {
        hash<AmqpReceiveOptions> ropts = opts ?? <AmqpReceiveOptions>{};
        ropts.shared = True;
        if (global) {
            ropts.global = True;
        }
        ropts.durable_name = subscription_name;
        string receiver = m_conn.createReceiver(address, ropts);
        return new AmqpConsumer(m_conn, receiver, subscription_name);
    }

    #! Create a durable consumer for a specific address
    /** @param address the AMQP address to consume from
        @param subscription_name the durable subscription name
        @param opts optional receiver options
        @param filter optional filter options

        @return a new AmqpConsumer with durable subscription

    */
    AmqpConsumer createDurableConsumer(string address, string subscription_name,
            *hash<AmqpReceiveOptions> opts, *hash<AmqpFilterOptions> filter) {
        string receiver = m_conn.createDurableReceiver(address, subscription_name,
            opts, filter);
        return new AmqpConsumer(m_conn, receiver, subscription_name);
    }

    #! Get a management client for broker introspection
    /** @return a new AmqpManagementClient

    */
    AmqpManagementClient getManagementClient() {
        return new AmqpManagementClient(m_conn);
    }

    #! Begin a transaction
    /** @throw AMQP-TRANSACTION-ERROR if a transaction is already active

    */
    beginTransaction() {
        m_conn.beginTransaction();
    }

    #! Commit the current transaction
    /** @throw AMQP-TRANSACTION-ERROR if no transaction is active

    */
    commitTransaction() {
        m_conn.commitTransaction();
    }

    #! Rollback the current transaction
    /** @throw AMQP-TRANSACTION-ERROR if no transaction is active

    */
    rollbackTransaction() {
        m_conn.rollbackTransaction();
    }

    #! Check if a transaction is active
    /** @return True if a transaction is active

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
