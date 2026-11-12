# Workflow nodes

The workflow model defines five node types. Every workflow embeds a root `RegionNode`, and regions can contain other regions. The workflow attributes and node state bindings are defined in [Workflow](WORKFLOW.md). [UML 2.5.1, chapters 13, 15, and 16](https://www.omg.org/spec/UML/2.5.1/PDF)

## `ActionNode`

![UML action shown as a rounded rectangle](images/action.svg)

Runs an arbitrary step and makes its outputs available to downstream nodes.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `action`. |
| `instruction` | `str` | Describes the work to perform. |
| `inputs` | [TypeMap](#typemap) | Defines the input attributes and their types. |
| `outputs` | [TypeMap](#typemap) | Defines the output attributes and their types. |

It maps to UML's `Action`, the base class for executable steps.

## `DecisionNode`

![UML decision shown as a diamond](images/decision.svg)

Evaluates its inputs using a user-defined instruction and selects a branch.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `decision`. |
| `instruction` | `str` | Describes how to choose a branch. |
| `inputs` | [TypeMap](#typemap) | Defines the input attributes and their types. |
| `branches` | `list[str]` | Defines a nonempty list of unique, user-defined string labels for this decision. |
| `output` | `str` | Is the selected branch name from `branches`. |

A decision must have at least one outgoing edge. Each branch label must match exactly one edge. Different labels can lead to the same downstream node.

It maps to UML's `DecisionNode` with guards on its outgoing edges.

## `SignalNode`

![UML send signal action shown as a pentagon pointing outward](images/signal.svg)

Signals an event and continues without waiting for a response.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `signal`. |
| `event_id` | `UUID` | Identifies the [event](EVENTS.md#event) to signal in the workflow's workspace. |
| `inputs` | [TypeMap](#typemap) | Describes the payload attributes and types declared by the event. |

See [signaling and receiving](EVENTS.md#signaling-and-receiving) for how the signal reaches matching waits.

It maps to UML's `SendSignalAction`.

## `WaitNode`

![UML accept event action shown as a notched pentagon and time wait shown as an hourglass](images/wait.svg)

Pauses the current path for a duration, until a timestamp, or until a matching event arrives.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `wait`. |
| `duration` | `timedelta` (optional) | Sets how long to wait. |
| `until` | `datetime` (optional) | Sets the timestamp at which the path resumes. |
| `event` | [EventSelector](#eventselector) (optional) | Selects the event that resumes the path. |
| `outputs` | [TypeMap](#typemap) (optional) | Describes the payload attributes and types when waiting for an event. |

Set exactly one of `duration`, `until`, or `event`.

The serialized time formats remain to be defined.

It maps to UML's `AcceptEventAction` with a `TimeEvent` or `SignalEvent` trigger.

## `RegionNode`

![UML structured region with initial and final nodes, expansion region with collection pins, and interruptible region with a lightning-bolt edge](images/region.svg)

Runs a nested graph and can repeat it once per item in a collection.

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the node. |
| `type` | `str` | Always `region`. |
| `nodes` | `list[Node]` | Contains the child node definitions. |
| `edges` | `list[Edge]` | Contains connections between child nodes. |
| `iteration` | `str` (optional) | Specifies the workflow state name or path containing the collection to iterate over. |

Each region has a built-in `initial` entry point and a `final` completion point. Reaching `final` in a nested region completes that region execution. Reaching `final` in the root region completes the workflow.

An iterating region completes normally when all iterations finish.

Traversing an edge from a node inside a region to a node outside it interrupts the region, stops its active child executions, and continues at the edge's target. For example, a `WaitNode` inside the region can receive an event and follow an edge out of the region.

In UML, nesting maps to `StructuredActivityNode`, iteration to `ExpansionRegion`, and interruption to `InterruptibleActivityRegion`. The root region corresponds to an `Activity`. The built-in `initial` and `final` points serve the roles of UML's `InitialNode` and `ActivityFinalNode`, scoped to the region in our model.

## Types

### `EventSelector`

Selects an event by UUID and optional payload conditions.

| Attribute | Type | Description |
| --- | --- | --- |
| `event_id` | `UUID` | Identifies the [event](EVENTS.md#event) in the workflow's workspace. |
| `filter` | [FilterMap](#filtermap) (optional) | Payload conditions. Omit or leave empty to accept any payload. |

```json
{
  "event_id": "b12d61af-d5d4-4d7a-8577-93adf18b5f10",
  "filter": {
    "order.id": {"=": "order-123"},
    "amount": {">=": 100, "<": 1000}
  }
}
```

### `FilterMap`

Maps attribute paths to one or more operator–operand pairs. Paths support nesting, such as `order.id`. All conditions must hold.

| Operator | Operand | Condition |
| --- | --- | --- |
| `=` | Value | Equals. |
| `!=` | Value | Differs from. |
| `>` | Number | Greater than. |
| `>=` | Number | Greater than or equal to. |
| `<` | Number | Less than. |
| `<=` | Number | Less than or equal to. |
| `in` | List of values | Equals any listed value. |
| `not in` | List of values | Differs from every listed value. |


### `TypeMap`

A map from attribute names to `str`, `bool`, `number`, or another `TypeMap` for nested attributes. `number` includes integers and floating-point numbers.

```json
{
  "name": "str",
  "address": {"city": "str"},
  "amount": "number",
  "approved": "bool"
}
```

### `DataMap`

A `DataMap` holds concrete values for the attributes declared by a [TypeMap](#typemap). Each value must match its declared type. If an attribute is declared as a nested `TypeMap`, its value is a nested `DataMap`.

This `DataMap` matches the `TypeMap` above.

```json
{
  "name": "Jordan",
  "address": {"city": "Oakland"},
  "amount": 125.50,
  "approved": true
}
```
