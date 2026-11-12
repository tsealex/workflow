# Events

Workflows use events to communicate that something happened. A `SignalNode` signals an `EventInstance` with the relevant payload values. A `WaitNode` in the same workspace can wait for a matching event instance and use its payload to resume the waiting path.

For example, the `review_completed` event declares an order ID and an approval result. When a review finishes, the review workflow signals the event with those values. The fulfillment workflow waiting for that order uses the approval result to decide whether to ship it.

## `Event`

An `Event` defines an event type and the shape of its payload.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the event. |
| `name` | `str` | Names the event. |
| `description` | `str` (optional) | Explains when the event is signaled and what it means. |
| `payload` | [TypeMap](NODES.md#typemap) | Defines the payload attributes and their types. Use an empty map for an event with no payload. |


## `EventInstance`

An `EventInstance` represents one occurrence of an `Event`. It references the event by UUID and records the actual payload values, when it was signaled, and which workflow execution signaled it.

| Attribute | Type | Description |
| --- | --- | --- |
| `event_id` | `UUID` | Identifies the event definition. |
| `instance_id` | `UUID` | Uniquely identifies this occurrence of the event. |
| `xid` | `UUID` | Identifies the workflow execution that signaled the event. |
| `notified_at` | `datetime` | Records when the event instance was signaled, in UTC. |
| `payload` | [DataMap](NODES.md#datamap) | Contains the values validated against the event's `TypeMap`. |
