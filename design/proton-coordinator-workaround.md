# Proton 0.40.0 Transaction Coordinator Workaround (PROTON-2825)

## Problem

Proton C++ 0.40.0 introduced a regression that prevents AMQP 1.0 transaction
coordinators from working when created via the C API within a C++ container.

### Root Cause

PROTON-2825 ("C++ binding treats transaction coordinator termini incorrectly")
was fixed in Proton 0.40.0 by adding this code to
`cpp/src/messaging_adapter.cpp` in the `on_link_remote_open()` function:

```cpp
// Currently don't implement (transaction) coordinator
if (pn_terminus_get_type(pn_link_remote_target(lnk))==PN_COORDINATOR) {
    auto error = pn_link_condition(lnk);
    pn_condition_set_name(error, "amqp:not-implemented");
    pn_link_close(lnk);
    return;
}
```

This was intended to reject **incoming** coordinator links from a remote peer
(server-side scenario). However, it also rejects **outgoing** coordinator links
that we create ourselves via the C API, because the check fires on
`PN_LINK_REMOTE_OPEN` for ALL links whose remote target is `PN_COORDINATOR` --
including links we initiated where the broker simply echoes back the coordinator
target type in its ATTACH response.

### Impact

- `beginTransaction()` fails with "timed out waiting for transaction
  coordinator credit" or "timed out waiting for transaction declaration"
- The coordinator link is closed with `amqp:not-implemented` before our
  `on_sender_open` or `on_sendable` handlers are called
- The broker accepts the coordinator correctly -- the rejection comes from the
  Proton C++ messaging adapter, not the broker

### Why This Only Affects Proton >= 0.40.0

The coordinator rejection code was added in commit 813f87ee (Proton 0.40.0).
Proton 0.39.0 and earlier do not have this check, so client-side coordinators
work correctly.

## Workaround

### Mechanism

The workaround exploits the Proton C++ container's event dispatch architecture:

1. `proactor_container_impl::dispatch()` calls `run_all_jobs()` on the
   connection's work queue **before** dispatching each event through the
   `messaging_adapter` (see `proactor_container_impl.cpp:562-567`)
2. We schedule a "guard" work item that checks the coordinator link's remote
   target type
3. When the broker's ATTACH response sets the remote target to
   `PN_COORDINATOR`, the guard changes it to `PN_TARGET` before the adapter's
   `on_link_remote_open()` runs its rejection check
4. Since the adapter sees `PN_TARGET` instead of `PN_COORDINATOR`, it processes
   the event normally and calls our `on_sender_open` handler

### Event Flow (with workaround)

```
1. beginTransaction() work callback:
   - Creates coordinator: pn_sender() + pn_terminus_set_type(PN_COORDINATOR) + pn_link_open()
   - Schedules guard work item

2. ATTACH frame sent to broker (target=@coordinator)

3. Broker responds with ATTACH (target=@coordinator) + FLOW (credit=1000)

4. Proton engine processes incoming data, sets remote target to PN_COORDINATOR,
   generates PN_LINK_REMOTE_OPEN event

5. Container dispatches PN_LINK_REMOTE_OPEN:
   a. run_all_jobs() -> guard runs -> sees PN_COORDINATOR -> changes to PN_TARGET
   b. messaging_adapter::on_link_remote_open() -> sees PN_TARGET -> does NOT reject
   c. handler.on_sender_open() called -> caches session

6. Container dispatches PN_LINK_FLOW:
   a. messaging_adapter::on_link_flow() -> handler.on_sendable() called
   b. Coordinator gets credit -> txn_coordinator_ready_ = true

7. Transaction Declare/Discharge messages proceed normally
```

### Why Changing the Remote Target Type Is Safe

The remote target terminus type is only meaningful during the ATTACH exchange.
After both sides have attached, the coordinator link's functionality is
determined by the messages sent on it (Declare descriptor 0x31, Discharge
descriptor 0x32) and the disposition responses (Declared descriptor 0x33). The
target type is never checked again after the ATTACH completes.

### Compile-Time Guard

The workaround is conditionally compiled:

```cpp
#if PN_VERSION_MAJOR > 0 || PN_VERSION_MINOR >= 40
    // workaround code
#endif
```

This ensures zero overhead on Proton < 0.40.0 where the issue does not exist.

## Proper Fix (for Proton upstream)

The fix in `messaging_adapter.cpp` should only reject coordinator links that are
**incoming** (peer-initiated), identified by `PN_LOCAL_UNINIT` state:

```cpp
// Fix: only reject incoming coordinator links, not client-initiated ones
if (pn_terminus_get_type(pn_link_remote_target(lnk))==PN_COORDINATOR
    && (pn_link_state(lnk) & PN_LOCAL_UNINIT)) {
    auto error = pn_link_condition(lnk);
    pn_condition_set_name(error, "amqp:not-implemented");
    pn_link_close(lnk);
    return;
}
```

Client-initiated coordinator links have `PN_LOCAL_ACTIVE` state by the time the
remote ATTACH arrives (because we called `pn_link_open()` before the broker
responded), so this check correctly distinguishes between the two cases.

## References

- [PROTON-2825](https://issues.apache.org/jira/browse/PROTON-2825) -- "C++
  binding treats transaction coordinator termini incorrectly"
- Proton commit 813f87ee -- "[C++ binding] Reject incoming transaction
  coordinator links"
- `QoreAmqpConnection::scheduleCoordinatorGuard()` -- workaround implementation
- `QoreAmqpConnection::beginTransaction()` -- calls the guard after coordinator
  creation
