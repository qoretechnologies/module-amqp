# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpManagementClient.qc AMQP Management Protocol client

/*  AmqpManagementClient.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! Client for broker introspection via the AMQP Management Protocol
/** Delegates to @ref Qore::Amqp::AmqpConnection "AmqpConnection" management methods.
    Returns empty results gracefully if the broker does not support management.

    @par Example:
    @code{.py}
AmqpClient client("amqp://guest:guest\@localhost:5672");
client.connect();

AmqpManagementClient mgmt = client.getManagementClient();
list<hash<AmqpAddressInfo>> addresses = mgmt.queryAddresses();
for (hash<AmqpAddressInfo> addr in addresses) {
    printf("Address: %s (type: %s, queues: %d, messages: %d)\n",
        addr.name, addr.routing_type, addr.queue_count, addr.message_count);
}

list<hash<AmqpQueueInfo>> queues = mgmt.queryQueues();
for (hash<AmqpQueueInfo> q in queues) {
    printf("Queue: %s (address: %s, messages: %d, consumers: %d)\n",
        q.name, q.address, q.message_count, q.consumer_count);
}

client.close();
    @endcode

    @since AmqpUtil 1.0
*/
public class AmqpManagementClient {
    private {
        #! The underlying connection
        AmqpConnection m_conn;
    }

    #! Create a new management client
    /** @param conn the AMQP connection

        @since AmqpUtil 1.0
    */
    constructor(AmqpConnection conn) {
        m_conn = conn;
    }

    #! Query all addresses from the broker
    /** @return a list of address info hashes, or an empty list if management is not supported

        @since AmqpUtil 1.0
    */
    list<hash<AmqpAddressInfo>> queryAddresses() {
        return m_conn.queryAddresses();
    }

    #! Query queues, optionally filtered by address
    /** @param address optional address to filter by

        @return a list of queue info hashes, or an empty list if management is not supported

        @since AmqpUtil 1.0
    */
    list<hash<AmqpQueueInfo>> queryQueues(*string address) {
        return m_conn.queryQueues(address);
    }

    #! Get detailed info about a specific address
    /** @param address the address name

        @return address info hash, or NOTHING if not found or management is not supported

        @since AmqpUtil 1.0
    */
    *hash<auto> getAddressInfo(string address) {
        return m_conn.getAddressInfo(address);
    }

    #! Get detailed info about a specific queue
    /** @param queue the queue name

        @return queue info hash, or NOTHING if not found or management is not supported

        @since AmqpUtil 1.0
    */
    *hash<auto> getQueueInfo(string queue) {
        return m_conn.getQueueInfo(queue);
    }
}

} # namespace Amqp
