# Workflow

A workflow contains a root region and defines the state available to its nodes.

## `Workflow`

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the workflow. |
| `root` | [RegionNode](NODES.md#regionnode) | Contains the workflow's nodes and edges. |
| `states` | [`JSONSchema`](#jsonschema) | Defines the workflow state object and its attributes. |
| `revision` | `int` | Increments whenever node changes are saved, including changes within nested regions. |
| `name` | `str` (optional) | Names the workflow. |
| `description` | `str` (optional) | Describes the workflow's purpose. |

A workflow execution starts at the root region’s `initial` node and completes when the root region completes. Each workflow execution has its own state values. See [Workflow execution](EXECUTION.md) for lifecycle and persistence.

Each node belongs to this workflow. Its saved changes are tracked by the workflow revision.

## Edges

See [Edges](EDGES.md) for edge definitions, region connections, and execution patterns.

## Node state bindings

The following attributes extend node instances within a workflow, including nodes in nested regions. Their base attributes are defined in [Workflow nodes](NODES.md).

### `ActionNode`

Reads selected workflow state values as inputs and writes its outputs back to workflow state.

| Attribute | Type | Description |
| --- | --- | --- |
| `code` | `str` | Stores the [Python code](#executable-interface) an LLM generates from `description` to run the action. |
| `input_states` | [list[str]](#state-paths) | Lists workflow state paths to read. |
| `output_states` | [list[str]](#state-paths) | Lists workflow state paths to write. |

### `DecisionNode`

Reads selected workflow state values as inputs and selects a branch.

| Attribute | Type | Description |
| --- | --- | --- |
| `code` | `str` | Stores the [Python code](#executable-interface) an LLM generates from `description` to select a branch. |
| `input_states` | [list[str]](#state-paths) | Lists workflow state paths to read. |

The `branch` value returned by `execute` determines the [outgoing edge](EDGES.md#branch-edge).

### `SignalNode`

Copies selected workflow state values into the event payload.

See [Events](EVENTS.md) for how a `WaitNode` receives a matching event instance in the same workspace.

| Attribute | Type | Description |
| --- | --- | --- |
| `input_states` | [list[str]](#state-paths) | Lists workflow state paths to copy into the event payload at the same paths. |

### `WaitNode`

When configured with `event`, copies selected values from the received payload into workflow state.

| Attribute | Type | Description |
| --- | --- | --- |
| `output_states` | [list[str]](#state-paths) (optional) | Lists workflow state paths to write from the same paths in the received payload. |

### `RegionNode`

Reads a collection from workflow state and runs the region's graph from `initial` to `final` once per item.

| Attribute | Type | Description |
| --- | --- | --- |
| `collection_state` | [str](#state-paths) (optional) | Specifies the workflow state path containing the collection to iterate over. Omit to run the region once. |
| `item_state` | `str` (optional) | Names the current item for child nodes to read. Required when `collection_state` is set. |

For each iteration, `item_state` refers to that iteration's collection item. For example, `item_state: "order"` lets child nodes read `order.total` through `input_states`.

Nodes in nested regions can also read the outer region's current item. An inner region can use `collection_state: "order.lines"` and `item_state: "line_item"`. Its child nodes can then read both `order.total` and `line_item.quantity`.

- Reject an `item_state` that conflicts with a top-level workflow state name or an enclosing region's `item_state`.
- Require `item_state` to be nonempty and contain no dots.
- Derive the item's schema from the selected collection's item schema.
- Treat the current item as a read-only input within its iteration.

For each item, execution starts at the region's `initial` node and completes that item's iteration at `final`. The region completes after all items finish. See [Region execution](EXECUTION.md#region-execution) for sequential collection processing.

## Executable interface

During compilation, the LLM uses the node’s `description` to generate a concrete Python subclass of `Executable` with both methods below. The workflow stores the generated class in the node’s `code` field. `WorkflowExecutor` calls `execute` for each `ActionNode` or `DecisionNode` execution. The validation harness calls `test_execute` during compilation.

```python
from abc import ABC, abstractmethod


class Executable(ABC):
    @abstractmethod
    async def execute(self, inputs: DataMap) -> DataMap:
        ...

    @abstractmethod
    async def test_execute(self) -> None:
        ...
```

- Generate idempotent action `execute` implementations so that continuing an interrupted node with its saved inputs does not duplicate external side effects.
- Pass the values selected by `input_states` to `execute` as a nested [DataMap](NODES.md#datamap).
- Return a nested `DataMap` containing the values at the paths in `output_states` for an `ActionNode`. Return an empty `DataMap` when the list is empty.
- A `DecisionNode` returns `{"branch": label}`, where `label` is a declared branch or `"default"`. The engine uses that label to select the outgoing edge.

### Code validation

The generated `test_execute` calls `execute` with representative inputs and asserts the expected outputs. It can temporarily replace external dependencies with test doubles. The validation harness runs `test_execute` in an isolated environment without live credentials and treats an exception as a validation failure.

## State paths

Use a state name for a top-level field and a dotted path for a nested field. Read paths must resolve to a property declared in `Workflow.states` or the [current item](#regionnode) of the node's region or an enclosing region. Write paths must resolve to properties declared in `Workflow.states`. An empty list selects no values.

For example, with `input_states: ["order.total"]`, the engine passes `{"order": {"total": 125.50}}` to `execute`. With `output_states: ["payment.approved"]`, the action returns `{"payment": {"approved": true}}` and the engine writes only `payment.approved`.

- Derive the types for code generation from the selected workflow state paths and collection item schemas.
- Omit `output_states` on duration and timestamp waits.

See [Node inputs and outputs](EXECUTION.md#node-inputs-and-outputs) for saved inputs, runtime validation, and the order in which outputs are applied.

## Workflow inputs

Workflow inputs are the initial state values supplied when starting a workflow execution. Callers can supply values for any attribute declared in `Workflow.states`, including attributes that nodes may write later.

Use data flow analysis to identify required workflow inputs. If a node can read a state path before any node has written it, mark that attribute as required in `Workflow.states`. For a required nested input, require each property along its path in the corresponding object schema.

Regions provide their current items during iteration, so callers do not supply values for `item_state` as workflow inputs.

See [Workflow execution](EXECUTION.md#node-inputs-and-outputs) for input validation and state updates.

## `JSONSchema`

```
JSONSchema := {
  "type": "object" | "array" | "string" | "number" | "boolean",
  "$ref": "#/$defs/TypeName", // optional reference to a named type
  "$defs": {
    TypeName: JSONSchema
  },
  "properties": {
    AttributeName: JSONSchema
  },
  "required": [AttributeName],
}
```

Event payload definitions and workflow state declarations use [JSON Schema](https://json-schema.org/draft/2020-12). Each contract has `"type": "object"` at its root. Define attributes with `properties` and list mandatory attributes in `required`. Apply the same rules to nested objects.

```json
{
  "type": "object",
  "properties": {
    "name": {"type": "string"},
    "address": {
      "type": "object",
      "properties": {
        "city": {"type": "string"}
      },
      "required": ["city"]
    },
    "amount": {"type": "number"},
    "approved": {"type": "boolean"}
  },
  "required": ["name", "address", "amount", "approved"]
}
```

### Empty object schema

Use this schema for a contract that accepts only an empty object.

```json
{
  "type": "object",
  "maxProperties": 0
}
```
