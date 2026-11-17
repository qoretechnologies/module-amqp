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

#! The exception code with which an observer refuses an AMQP message permanently, so that it is rejected
/** see @ref amqpeventdataprovider_settlement */
public const AMQP_MESSAGE_REJECTED_ERR = "AMQP-MESSAGE-REJECTED";

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

    @anchor amqpeventdataprovider_settlement
    @par Settlement
    Every delivery is settled once the observers have handled it (qore#6396):
    - it is accepted when every observer accepted it;
    - an observer refuses a message by throwing from its \c update() method (it reaches the provider as an
      \c OBSERVER-NOTIFICATION-ERROR); a refused message is settled as \c modified with \c delivery-failed set,
      so that the broker counts the attempt and delivers it again (or dead-letters it when its maximum number of
      delivery attempts is reached, where the broker is configured so), after a back-off that starts at
      \c refusal_delay_ms and doubles with each consecutive refusal (of any message) up to
      \c max_refusal_delay_ms; the first successful delivery resets the back-off.  A redelivered message goes to
      every observer again, so an observer that accepted it before another one refused it sees it again
      (at-least-once delivery);
    - a message that every failing observer refuses permanently, by throwing @ref AMQP_MESSAGE_REJECTED_ERR, is
      rejected, and the broker does not deliver it again (it dead-letters it where it is configured so); so is a
      message whose fields cannot be decoded, which no observer could handle.

    A refusal never stops the provider.  When the link has credit for more than one message, messages received
    after a refused one can be delivered before it is delivered again.
*/
public class AmqpEventDataProvider inherits AbstractDataProvider, DelayedObservable {
    public {
        #! Provider info
        const ProviderInfo = <DataProviderInfo>{
            "type": "AmqpEventDataProvider",
            "supports_observable": True,
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

        #! Receive loop running flag (guards against multiple background threads)
        bool m_running = False;

        #! Wakes the back-off of a refusal when the provider is stopped
        Mutex m_lock();
        Condition m_cond();

        #! Counts the running receive loop, so that stopEvents() can wait for it to end
        Counter m_loop_cnt();

        #! The thread ID of the receive loop
        int m_loop_tid;

        #! The initial delay in milliseconds before a refused message is settled for redelivery
        int m_refusal_delay_ms = DefaultRefusalDelayMs;

        #! The maximum delay in milliseconds before a refused message is settled for redelivery
        int m_max_refusal_delay_ms = DefaultMaxRefusalDelayMs;

        #! Settlement counters; see getDeliveryStats()
        hash<string, int> m_stats = {
            "accepted": 0,
            "refused": 0,
            "rejected": 0,
            "settle_errors": 0,
        };
    }

    public {
        #! The default initial delay in milliseconds before a refused message is settled for redelivery
        const DefaultRefusalDelayMs = 1000;

        #! The default maximum delay in milliseconds before a refused message is settled for redelivery
        const DefaultMaxRefusalDelayMs = 60000;

        #! The longest time in milliseconds that one receive call waits, so that a stop request is seen promptly
        const MaxReceiveSliceMs = 1000;

        #! The time in milliseconds to wait after a receive error before receiving again
        const ReceiveErrorDelayMs = 1000;
    }

    #! Creates the event data provider
    /** @param client the AMQP client
        @param address the AMQP address to receive from
        @param timeout_ms default timeout
        @param opts optional settings:
        - \c refusal_delay_ms: the initial delay in milliseconds before a refused message is settled for redelivery
          (default @ref DefaultRefusalDelayMs)
        - \c max_refusal_delay_ms: the maximum delay in milliseconds before a refused message is settled for
          redelivery (default @ref DefaultMaxRefusalDelayMs)
    */
    constructor(Amqp::AmqpClient client, string address, int timeout_ms, *hash<auto> opts) {
        m_client = client;
        m_address = address;
        m_timeout_ms = timeout_ms;
        m_refusal_delay_ms = max(1, int(opts.refusal_delay_ms ?? DefaultRefusalDelayMs));
        m_max_refusal_delay_ms = max(m_refusal_delay_ms,
            int(opts.max_refusal_delay_ms ?? DefaultMaxRefusalDelayMs));
    }

    #! Returns settlement statistics
    /** @return a hash with the following keys:
        - \c accepted: the number of accepted messages
        - \c refused: the number of refusals (each refused message is settled for redelivery)
        - \c rejected: the number of messages refused permanently and rejected
        - \c settle_errors: the number of settlements that failed (the broker delivers those messages again when
          the link is closed)
    */
    hash<string, int> getDeliveryStats() {
        AutoLock al(m_lock);
        return m_stats;
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
    hash<string, hash<DataProviderMessageInfo>> getEventTypesImpl() {
        return {
            EVENT_AMQP_MESSAGE: <DataProviderMessageInfo>{
                "desc": "Fired when a message is received on the AMQP address",
                "type": new AmqpMessageEventDataType(),
            },
        };
    }

    #! Returns example event data
    private auto getExampleEventDataImpl(string event_id) {
        return {
            "body": "Hello, AMQP!",
            "content_type": "text/plain",
            "message_id": "msg-001",
            "correlation_id": NOTHING,
            "reply_to": NOTHING,
            "subject": "example",
            "application_properties": {"source": "example"},
        };
    }

    #! Called when observers are ready to receive events
    observersReady() {
        {
            AutoLock al(m_lock);
            # a stopped provider is not started again
            if (m_running || m_stop) {
                return;
            }
            m_running = True;
            m_loop_cnt.inc();
        }
        on_error {
            AutoLock al(m_lock);
            m_running = False;
            m_loop_cnt.dec();
        }
        # Start receiving in a background thread
        background receiveLoop();
    }

    #! Stops event delivery
    /** Waits until the receive loop has ended, so that the delivery being handled is settled first, unless it is
        called by an observer in the receive loop itself
    */
    stopEvents() {
        {
            AutoLock al(m_lock);
            m_stop = True;
            m_running = False;
            m_cond.broadcast();
        }
        if (gettid() != m_loop_tid) {
            m_loop_cnt.waitForZero();
        }
    }

    #! Returns static provider info
    private hash<DataProviderInfo> getStaticInfoImpl() {
        return ProviderInfo;
    }

    #! Background receive loop
    /** see @ref amqpeventdataprovider_settlement */
    private receiveLoop() {
        {
            AutoLock al(m_lock);
            m_loop_tid = gettid();
        }
        on_exit {
            AutoLock al(m_lock);
            m_loop_tid = 0;
            m_running = False;
            m_loop_cnt.dec();
        }
        # consecutive refusals; gives the back-off before the next redelivery
        int refusals = 0;
        # the receive waits in slices, so that a stop request is seen promptly
        int receive_ms = max(1, min(m_timeout_ms, MaxReceiveSliceMs));
        while (!m_stop) {
            *AmqpMessage msg;
            try {
                msg = m_client.receive(m_address, receive_ms);
            } catch (hash<ExceptionInfo> ex) {
                if (m_stop) {
                    break;
                }
                Logger::LoggerWrapper::error("AmqpEventDataProvider %s: receive error: %s: %s", m_address, ex.err,
                    ex.desc);
                # the connection can be down: wait before receiving again
                waitBeforeRetry(ReceiveErrorDelayMs);
                continue;
            }
            if (!msg) {
                continue;
            }
            *binary tag = msg.getDeliveryTag();
            hash<auto> event_data;
            try {
                event_data = {
                    "body": msg.getBody(),
                    "content_type": msg.getContentType(),
                    "message_id": msg.getMessageId(),
                    "correlation_id": msg.getCorrelationId(),
                    "reply_to": msg.getReplyTo(),
                    "subject": msg.getSubject(),
                    "application_properties": msg.getApplicationProperties(),
                };
            } catch (hash<ExceptionInfo> ex) {
                # no observer can handle a message whose fields cannot be decoded: it is a permanent failure
                Logger::LoggerWrapper::error("AmqpEventDataProvider %s: a message cannot be decoded; it is rejected: "
                    "%s: %s", m_address, ex.err, ex.desc);
                settle("rejected", tag, \m_client.reject());
                continue;
            }
            try {
                notifyObservers(EVENT_AMQP_MESSAGE, event_data);
            } catch (hash<ExceptionInfo> ex) {
                if (isPermanentRefusal(ex)) {
                    Logger::LoggerWrapper::error("AmqpEventDataProvider %s: message %y refused permanently; it is "
                        "rejected: %s: %s", m_address, event_data.message_id, ex.err, ex.desc);
                    settle("rejected", tag, \m_client.reject());
                    continue;
                }
                Logger::LoggerWrapper::error("AmqpEventDataProvider %s: message %y refused; it will be delivered "
                    "again: %s: %s", m_address, event_data.message_id, ex.err, ex.desc);
                backOff(++refusals);
                settle("refused", tag, sub (binary t) { m_client.modify(t, True, False); });
                continue;
            }
            refusals = 0;
            settle("accepted", tag, \m_client.accept());
        }
    }

    #! Returns @ref True if every failing observer refused the message permanently
    private static bool isPermanentRefusal(hash<ExceptionInfo> ex) {
        if (ex.err != "OBSERVER-NOTIFICATION-ERROR") {
            return ex.err == AMQP_MESSAGE_REJECTED_ERR;
        }
        list<auto> errs = ex.arg.typeCode() == NT_LIST ? ex.arg : ();
        if (!errs) {
            return False;
        }
        foreach auto e in (errs) {
            if (e.typeCode() != NT_HASH || e.err != AMQP_MESSAGE_REJECTED_ERR) {
                return False;
            }
        }
        return True;
    }

    #! Settles a delivery and counts it
    /** @param outcome the counter of the settlement
        @param tag the delivery tag; a message without one cannot be settled
        @param action the settlement call
    */
    private settle(string outcome, *binary tag, code action) {
        {
            AutoLock al(m_lock);
            ++m_stats{outcome};
        }
        if (!tag) {
            return;
        }
        try {
            action(tag);
        } catch (hash<ExceptionInfo> ex) {
            # the link failed, for example: the broker delivers the message again when it is closed
            {
                AutoLock al(m_lock);
                ++m_stats.settle_errors;
            }
            Logger::LoggerWrapper::error("AmqpEventDataProvider %s: cannot settle a delivery as %s: %s: %s",
                m_address, outcome, ex.err, ex.desc);
        }
    }

    #! Waits before a refused message is settled for redelivery; returns early when the provider is stopped
    /** @param attempt the number of consecutive refusals
    */
    private backOff(int attempt) {
        int delay_ms = m_refusal_delay_ms;
        while (--attempt > 0 && delay_ms < m_max_refusal_delay_ms) {
            delay_ms *= 2;
        }
        waitBeforeRetry(min(delay_ms, m_max_refusal_delay_ms));
    }

    #! Waits for the given time; returns early when the provider is stopped
    private waitBeforeRetry(int delay_ms) {
        date end = now_us() + milliseconds(delay_ms);
        AutoLock al(m_lock);
        while (!m_stop) {
            int remaining_ms = (end - now_us()).durationMilliseconds();
            if (remaining_ms <= 0) {
                break;
            }
            m_cond.wait(m_lock, remaining_ms);
        }
    }
}

} # namespace AmqpDataProvider
