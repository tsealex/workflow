# Events

Workflows use events to communicate that something happened. A `SignalNode` signals an `EventInstance` with the relevant payload values. A `WaitNode` in the same workspace waits for a matching event instance. When one arrives, execution continues with the received payload.

For example, the `review_completed` event declares an order ID and an approval result. When a review finishes, the review workflow signals the event with those values. The fulfillment workflow waits for a review result for that order, then uses the approval result to decide whether to ship it.

## Event delivery

An event-based `WaitNode` begins waiting when its execution starts. If an event instance matches the node's selector and is committed while the node is waiting, the wait ends and execution continues. The same event instance can satisfy multiple matching waits in the workspace.

Canceling the node execution also ends the wait. Each visit to the node waits for events committed after that visit begins waiting.

## `Event`

An `Event` defines an event type and the shape of its payload.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the event. |
| `name` | `str` | Summarizes the event for display. |
| `description` | `str` (optional) | Explains when the event is signaled and what it means. |
| `payload` | [`JSONSchema`](WORKFLOW.md#jsonschema) | Defines the payload attributes and their types. Use the [empty-object schema](WORKFLOW.md#empty-object-schema) for an event with no payload. |

## `EventInstance`

An `EventInstance` represents one occurrence of an `Event`. It references the event by UUID and records the actual payload values, when it was signaled, and which workflow execution signaled it.

| Attribute | Type | Description |
| --- | --- | --- |
| `event_id` | `UUID` | Identifies the event definition. |
| `instance_id` | `UUID` | Uniquely identifies this occurrence of the event. |
| `xid` | `UUID` | Identifies the workflow execution that signaled the event. |
| `notified_at` | `datetime` | Records when the event instance was signaled, in UTC. |
| `payload` | [DataMap](NODES.md#datamap) | Contains the values validated against the event's payload schema. |

## Timer waits

### Duration

A `WaitNode` with `duration` waits for the specified interval, measured from when the wait begins. Execution then continues.

### Timestamp

A `WaitNode` with `until` waits until the specified time, then execution continues. If that time has already passed when the node starts, execution can continue immediately.
