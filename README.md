# Qore AMQP Module

An AMQP 1.0 messaging module for [Qore](https://qore.org) using [Apache Qpid Proton](https://qpid.apache.org/proton/) C++.

## Features

- AMQP 1.0 send/receive with full bidirectional type mapping
- Delivery disposition (accept, reject, release, modify)
- Transactions (begin, commit, rollback)
- Durable subscriptions and message selectors/filters
- TLS/SSL (`amqps://`) and SASL authentication
- Broker introspection via the AMQP Management Protocol
- High-level client API (`AmqpUtil` module)
- Data provider integration (`AmqpDataProvider` module)

## Architecture

The module has three layers:

| Layer | Type | Description |
|-------|------|-------------|
| `amqp` | Binary (C++) | Core `AmqpConnection` and `AmqpMessage` classes wrapping qpid-proton-cpp |
| `AmqpUtil` | Pure Qore | High-level `AmqpClient`, `AmqpProducer`, `AmqpConsumer`, `AmqpRequestReply`, `AmqpManagementClient` |
| `AmqpDataProvider` | Pure Qore | Data provider factory, connection scheme (`amqp://`, `amqps://`), observable events |

## Quick Start

### Simple Send and Receive

```qore
%modern
%requires amqp

AmqpConnection conn(<AmqpConnectionOptions>{"url": "amqp://guest:guest@localhost:5672"});
conn.connect();

string sender = conn.createSender("test-queue");
conn.send(sender, new AmqpMessage("Hello, AMQP!"));

string receiver = conn.createReceiver("test-queue");
*AmqpMessage msg = conn.receive(receiver, 5s);
if (msg) {
    printf("Received: %y\n", msg.getBody());
}
conn.close();
```

### Using the High-Level Client (AmqpUtil)

```qore
%modern
%requires AmqpUtil

Amqp::AmqpClient client("amqp://guest:guest@localhost:5672");
client.connect();

# Simple send and receive
client.send("my-queue", "Hello!");
*AmqpMessage msg = client.receive("my-queue", 5s);

# Producer/consumer pattern
Amqp::AmqpProducer producer = client.createProducer("my-queue");
producer.send("Message body", <AmqpMessageProperties>{"content_type": "text/plain"});

Amqp::AmqpConsumer consumer = client.createConsumer("my-queue");
*AmqpMessage received = consumer.receive(10s);
if (received) {
    consumer.accept(received.getDeliveryTag());
}

client.close();
```

### Transactions

```qore
%modern
%requires AmqpUtil

Amqp::AmqpClient client("amqp://guest:guest@localhost:5672");
client.connect();

client.beginTransaction();
client.send("queue1", "transactional message 1");
client.send("queue2", "transactional message 2");
client.commitTransaction();  # Both messages delivered atomically

# Or rollback
client.beginTransaction();
client.send("queue1", "this will be rolled back");
client.rollbackTransaction();  # Message not delivered

client.close();
```

### Request-Reply

```qore
%modern
%requires AmqpUtil

Amqp::AmqpClient client("amqp://guest:guest@localhost:5672");
client.connect();

Amqp::AmqpRequestReply rr(client, "request-queue");
AmqpMessage response = rr.request("What is the answer?", NOTHING, 30s);
printf("Reply: %y\n", response.getBody());
rr.close();
client.close();
```

### Durable Subscriptions

```qore
%modern
%requires AmqpUtil

Amqp::AmqpClient client("amqp://guest:guest@localhost:5672");
client.connect();

# Create a durable consumer that survives disconnects
Amqp::AmqpConsumer consumer = client.createDurableConsumer("topic/news", "my-subscription");
*AmqpMessage msg = consumer.receive(10s);

# Close without unsubscribing (messages accumulate while disconnected)
consumer.closeDurable();
client.close();

# Later: reconnect and resume receiving
# To permanently remove the subscription:
# consumer.unsubscribe();
```

### Message Selectors

```qore
%modern
%requires AmqpUtil

Amqp::AmqpClient client("amqp://guest:guest@localhost:5672");
client.connect();

# Only receive messages where application property "color" = "red"
Amqp::AmqpConsumer consumer = client.createConsumer("my-queue",
    NOTHING,
    <AmqpFilterOptions>{"selector": "color = 'red'"});
*AmqpMessage msg = consumer.receive(10s);

client.close();
```

### TLS/SSL

```qore
%modern
%requires amqp

AmqpConnection conn(<AmqpConnectionOptions>{
    "url": "amqps://broker.example.com:5671",
    "ssl": <AmqpSslOptions>{
        "ca_cert": "/path/to/ca.crt",
        "client_cert": "/path/to/client.crt",
        "client_key": "/path/to/client.key",
        "verify": True,
    },
});
conn.connect();
```

### SASL Authentication

```qore
%modern
%requires amqp

AmqpConnection conn(<AmqpConnectionOptions>{
    "url": "amqp://broker.example.com:5672",
    "sasl": <AmqpSaslOptions>{
        "mechanism": "PLAIN",
        "username": "myuser",
        "password": "mypassword",
    },
});
conn.connect();
```

### Broker Introspection (Management Protocol)

```qore
%modern
%requires AmqpUtil

Amqp::AmqpClient client("amqp://guest:guest@localhost:5672");
client.connect();

Amqp::AmqpManagementClient mgmt = client.getManagementClient();
list<hash<AmqpAddressInfo>> addresses = mgmt.queryAddresses();
for (int i = 0; i < addresses.size(); ++i) {
    printf("Address: %s (routing: %s, queues: %d)\n",
        addresses[i].name, addresses[i].routing_type, addresses[i].queue_count);
}

client.close();
```

### Data Provider

```qore
%modern
%requires AmqpDataProvider

# Create via factory
AbstractDataProviderFactory factory = DataProvider::getFactory("amqp");
AbstractDataProvider provider = factory.create({
    "url": "amqp://guest:guest@localhost:5672",
    "addresses": "queue1,queue2",
});

# Send a message
auto result = provider.getChildProvider("queue1").doRequest({"body": "Hello via DataProvider!"});

# Auto-discover addresses from broker
AbstractDataProvider mgmt_provider = factory.create({
    "url": "amqp://guest:guest@localhost:5672",
    "use_management": True,
});
```

## Type Mapping

| AMQP Type | Qore Type |
|-----------|-----------|
| string | string |
| binary | binary |
| boolean | bool |
| byte/short/int/long | int |
| float/double | float |
| timestamp | date |
| uuid | string (formatted) |
| symbol | string |
| map | hash |
| list | list |
| null | NOTHING |

## Building

### Prerequisites

- Qore 2.0+
- Apache Qpid Proton C++ (`libqpid-proton-cpp-dev` on Ubuntu, `qpid-proton-cpp-dev` on Alpine)
- C++17 compiler

### Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=debug
make -j4
make install
```

### Running Tests

Unit tests (no broker required):
```bash
qore --enable-debug test/amqp.qtest -vv
```

Integration tests (requires an AMQP 1.0 broker such as Apache ActiveMQ Artemis):
```bash
export AMQP_TEST_URL="amqp://guest:guest@localhost:5672"
qore --enable-debug test/amqp-integration.qtest -vv
qore --enable-debug test/AmqpUtil.qtest -vv
qore --enable-debug test/AmqpDataProvider.qtest -vv
```

## License

MIT License - see [COPYING.MIT](COPYING.MIT) for details.

Copyright (C) 2026 Qore Technologies, s.r.o.
