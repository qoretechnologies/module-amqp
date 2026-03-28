# -*- mode: qore; indent-tabs-mode: nil -*-
#! @file AmqpConnectionObservable.qc Observable wrapper for AMQP connection events

/*  AmqpConnectionObservable.qc Copyright 2026 Qore Technologies, s.r.o.

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

#! Main Amqp namespace
public namespace Amqp {

#! Connection event: connection established
public const EVENT_AMQP_CONNECTED = "amqp-connected";
#! Connection event: connection lost
public const EVENT_AMQP_DISCONNECTED = "amqp-disconnected";
#! Connection event: connection or transport error
public const EVENT_AMQP_ERROR = "amqp-error";
#! Connection event: reconnection succeeded
public const EVENT_AMQP_RECONNECTED = "amqp-reconnected";

#! Observable wrapper for AMQP connection lifecycle events
/** Polls the underlying AmqpConnection for events and dispatches them to
    registered observers.

    @par Example
    @code{.py}
AmqpClient client("amqp://localhost:5672");
AmqpConnectionObservable obs(client.getConnection());
obs.registerObserver("amqp-connected", sub (string event_id, hash<auto> data) {
    printf("Connected at %y\n", data.timestamp);
});
client.connect();
obs.checkEvents();  # dispatches pending events
    @endcode
*/
public class AmqpConnectionObservable inherits Observable {
    private {
        #! The underlying connection
        AmqpConnection m_conn;
    }

    #! Creates the observable wrapper
    /** @param conn the AMQP connection to observe
    */
    constructor(AmqpConnection conn) {
        m_conn = conn;
    }

    #! Poll for pending connection events and dispatch to observers
    /** Call this periodically or after operations that may trigger events.
    */
    checkEvents() {
        list<hash<auto>> events = m_conn.getConnectionEvents();
        for (hash<auto> ev in events) {
            notifyObservers(ev.event, ev);
        }
    }
}

} # namespace Amqp
