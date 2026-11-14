# Workflow nodes

The workflow model defines five node types. Every workflow embeds a root `RegionNode`, and regions can contain other regions. The workflow attributes and node state bindings are defined in [Workflow](WORKFLOW.md). [UML 2.5.1, chapters 13, 15, and 16](https://www.omg.org/spec/UML/2.5.1/PDF)

## `ActionNode`

![UML action shown as a rounded rectangle](images/action.svg)

Runs an arbitrary step and makes its outputs available to downstream nodes.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `action`. |
| `name` | `str` | Summarizes the description for display. |
| `description` | `str` | Describes the work to perform. |

It maps to UML's `Action`, the base class for executable steps.

## `DecisionNode`

![UML decision shown as a diamond](images/decision.svg)

Selects a branch based on its inputs and `description`. The execution result contains a `branch` value from `branches` or `default`.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `decision`. |
| `name` | `str` | Summarizes the description for display. |
| `description` | `str` | Describes how to choose a branch. |
| `branches` | `list[str]` | Defines unique, user-defined branch labels. The list may be empty. The label `default` is reserved. |

See [Branch edge](EDGES.md#branch-edge) for how branch labels determine outgoing edges.

It maps to UML's `DecisionNode` with guards on its outgoing edges.

## `SignalNode`

![UML send signal action shown as a pentagon pointing outward](images/signal.svg)

Signals an event and continues without waiting for a response.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `signal`. |
| `name` | `str` | Summarizes the node’s purpose for display. |
| `event_id` | `UUID` | Identifies the [event](EVENTS.md#event) to signal in the workflow's workspace. |

See [Events](EVENTS.md) for how the signal reaches matching waits.

It maps to UML's `SendSignalAction`.

## `WaitNode`

![UML accept event action shown as a notched pentagon and time wait shown as an hourglass](images/wait.svg)

Pauses execution along the current path for a specified duration, until a specified time, or until a matching event arrives.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `wait`. |
| `name` | `str` | Summarizes the node’s purpose for display. |
| `duration` | `timedelta` (optional) | Sets the [duration](EVENTS.md#duration) to wait. |
| `until` | `datetime` (optional) | Sets the [timestamp](EVENTS.md#timestamp) at which execution can continue. |
| `event` | [EventSelector](#eventselector) (optional) | Selects the event instances that end the wait. |

Set exactly one of `duration`, `until`, or `event`.

Duration and timestamp waits receive no event payload. An event-based wait can write values from the matching `EventInstance` payload through its [state bindings](WORKFLOW.md#waitnode).

An event-based wait begins listening when its execution starts. See [Event delivery](EVENTS.md#event-delivery) for how matching events allow execution to continue.

The formats used to serialize durations and timestamps remain to be defined.

It maps to UML's `AcceptEventAction` with a `TimeEvent` or `SignalEvent` trigger.

## `RegionNode`

![UML structured region with initial and final nodes, expansion region with collection pins, and interruptible region with a lightning-bolt edge](images/region.svg)

Runs a nested graph and can repeat it once per item in a collection. `WorkflowEngine` exclusively handles region execution and processes collection items [sequentially](EXECUTION.md#region-execution).

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `region`. |
| `name` | `str` | Summarizes the node’s purpose for display. |
| `nodes` | `list[Node]` | Contains the child node definitions. |
| `edges` | [`list[Edge]`](EDGES.md#edge) | Contains the [edges](EDGES.md#edges) owned by the region. |

Each region has a built-in `initial` node for entry and a `final` node for completion. Each has its own UUID. Reaching `final` completes the region unless it iterates over a collection. When the root region completes, the workflow execution completes.

A node belongs to the region whose `nodes` list contains it. When loading a workflow revision, `WorkflowEngine` can build a lookup from each node ID to its enclosing region ID. The root region has no enclosing region.

A region with the [`collection_state` binding](WORKFLOW.md#regionnode) runs its graph from `initial` to `final` once per item in the collection. Reaching `final` completes that item's iteration. The region completes normally after all iterations finish. Cycles within the region repeat nodes for the current collection item. Reaching `final` ends that item's iteration.

See [Interruptible edge](EDGES.md#interruptible-edge) for how an edge can exit a region before normal completion.

In UML, nesting maps to `StructuredActivityNode`, iteration to `ExpansionRegion`, and interruption to `InterruptibleActivityRegion`. The root region corresponds to an `Activity`. The built-in `initial` and `final` nodes serve the roles of UML's `InitialNode` and `ActivityFinalNode`, scoped to the region in our model.

## Types

### `EventSelector`

```
EventSelector := {
  "event_id": UUID,
  "filter": FilterMap // optional
}
```

Selects an event by UUID and optional payload conditions.

| Attribute | Type | Description |
| --- | --- | --- |
| `event_id` | `UUID` | Identifies the [event](EVENTS.md#event) in the workflow's workspace. |
| `filter` | [FilterMap](#filtermap) (optional) | Payload conditions. Omit or leave empty to accept any payload. |

```json
{
  "event_id": "b12d61af-d5d4-4d7a-8577-93adf18b5f10",
  "filter": {
    "order.id": {"eq": "order-123"},
    "amount": {"gte": 100, "lt": 1000}
  }
}
```

### `FilterMap`

```
FilterMap := {
  AttributePath: {
    Operator: Operand
  }
}
```

Maps attribute paths to one or more operator–operand pairs. Paths support nesting, such as `order.id`. All conditions must hold.

| Operator | Operand | Condition |
| --- | --- | --- |
| `eq` | Value | Equals. |
| `ne` | Value | Differs from. |
| `gt` | Number | Greater than. |
| `gte` | Number | Greater than or equal to. |
| `lt` | Number | Less than. |
| `lte` | Number | Less than or equal to. |
| `in` | List of values | Equals any listed value. |
| `not_in` | List of values | Differs from every listed value. |

### `DataMap`

```
DataMap := {
  AttributePath: Value
}
```

A `DataMap` is a Python dictionary containing concrete values. When the input arrives as raw JSON text, parse it with `json.loads()` before validating the resulting value against its contract with `jsonschema`. After validation, the caller uses the parsed dictionary.

This `DataMap` matches the [`JSONSchema` example](WORKFLOW.md#jsonschema).

```json
{
  "name": "Jordan",
  "address": {"city": "Oakland"},
  "amount": 125.50,
  "approved": true
}
```
