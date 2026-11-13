# Events

Workflows use events to communicate that something happened. A `SignalNode` signals an `EventInstance` with the relevant payload values. A `WaitNode` in the same workspace can wait for a matching event instance and use its payload to resume the waiting path.

For example, the `review_completed` event declares an order ID and an approval result. When a review finishes, the review workflow signals the event with those values. The fulfillment workflow waiting for that order uses the approval result to decide whether to ship it.

## Event delivery

`WorkflowEngine` commits the event instance when a `SignalNode` runs. An event instance can resume a matching `WaitNode` only if the wait was active before the signal was committed. The same event instance can resume multiple matching waits in the workspace.

## `Event`

An `Event` defines an event type and the shape of its payload.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the event. |
| `name` | `str` | Names the event. |
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

A `WaitNode` with `duration` resumes its path after the specified interval elapses from the start of the wait.

### Timestamp

A `WaitNode` with `until` resumes its path when the specified timestamp is reached. If the timestamp has already passed when the wait starts, the path can resume immediately.
