# Workflow

A workflow embeds a root region and defines the states used by its nodes.

## `Workflow`

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the workflow. |
| `root` | [RegionNode](NODES.md#regionnode) | Contains the workflow's nodes and edges. |
| `states` | [`JSONSchema`](#jsonschema) | Defines the workflow state object and its attributes. |
| `revision` | `int` | Increments whenever node changes are saved, including changes within nested regions. |
| `name` | `str` (optional) | Names the workflow. |
| `description` | `str` (optional) | Describes the workflow's purpose. |

A workflow execution starts at the root region’s `initial` node and completes when the root region completes. Each workflow execution holds its own state values. See [Workflow execution](EXECUTION.md) for lifecycle and persistence.

Each node belongs to this workflow. Its saved changes are tracked by the workflow revision.

## Edges

See [Edges](EDGES.md) for edge definitions, region connections, and execution patterns.

## Node state bindings

The following attributes extend node instances within a workflow, including nodes in nested regions. Their base attributes are defined in [Workflow nodes](NODES.md).

### `ActionNode`

Reads workflow states into its inputs and writes its outputs to workflow states.

| Attribute | Type | Description |
| --- | --- | --- |
| `code` | `str` | Stores the [Python code](#executable-interface) an LLM generates from `description` to run the action. |
| `input_states` | [list[str]](#state-paths) | Lists workflow state paths to read. |
| `output_states` | [list[str]](#state-paths) | Lists workflow state paths to write. |

### `DecisionNode`

Reads workflow states into its inputs and selects a branch.

| Attribute | Type | Description |
| --- | --- | --- |
| `code` | `str` | Stores the [Python code](#executable-interface) an LLM generates from `description` to select a branch. |
| `input_states` | [list[str]](#state-paths) | Lists workflow state paths to read. |

The `branch` value returned by `execute` determines the [outgoing edge](EDGES.md#branch-edge).

### `SignalNode`

Reads workflow states to populate the event payload.

See [Events](EVENTS.md) for how a `WaitNode` receives a matching event instance in the same workspace.

| Attribute | Type | Description |
| --- | --- | --- |
| `input_states` | [list[str]](#state-paths) | Lists workflow state paths to copy into the event payload at the same paths. |

### `WaitNode`

When configured with `event`, writes received payload attributes to workflow states.

| Attribute | Type | Description |
| --- | --- | --- |
| `output_states` | [list[str]](#state-paths) (optional) | Lists workflow state paths to write from the same paths in the received payload. |

### `RegionNode`

Reads a collection from workflow state and runs the region's graph from `initial` to `final` once per item.

| Attribute | Type | Description |
| --- | --- | --- |
| `foreach_state` | [str](#state-paths) (optional) | Specifies the workflow state path containing the collection to iterate over. Omit to run the region once. |

For each item, execution starts at the region's `initial` node and completes that item's iteration at `final`. The region completes after all items finish. See [RegionNode](NODES.md#regionnode) for region execution semantics.

## Executable interface

During compilation, the LLM uses the node’s `description` to generate a concrete Python subclass of `Executable` with both methods below. The workflow stores the generated class in the node’s `code` field. `WorkflowExecutor` calls `execute` for each `ActionNode` or `DecisionNode` execution attempt. The validation harness calls `test_execute` during compilation.

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

- The LLM should generate idempotent `execute` implementations where possible so that retrying the same node visit does not duplicate external side effects.
- Pass the values selected by `input_states` to `execute` as a nested [DataMap](NODES.md#datamap).
- Return a nested `DataMap` containing the values at the paths in `output_states` for an `ActionNode`. Return an empty `DataMap` when the list is empty.
- A `DecisionNode` returns `{"branch": label}`, where `label` is a declared branch or `"default"`. The engine uses that label to select the outgoing edge.
- Validate each selected input against its schema in `Workflow.states` before calling `execute`.
- Report the attempt as failed if input validation fails, `execute` raises an exception, or its return value fails the applicable output validation.

### Code validation

The generated `test_execute` calls `execute` with representative inputs and asserts the expected outputs. It can temporarily replace external dependencies with test doubles. The validation harness runs `test_execute` in an isolated environment without live credentials and treats an exception as a validation failure.

## State paths

Use a state name for a top-level field and a dotted path for a nested field. Each path must resolve to a property declared in `Workflow.states`. An empty list selects no values.

For example, with `input_states: ["order.total"]`, the engine passes `{"order": {"total": 125.50}}` to `execute`. With `output_states: ["payment.approved"]`, the action returns `{"payment": {"approved": true}}` and the engine writes only `payment.approved`.

- Derive the types for code generation from the selected paths in `Workflow.states`.
- Fail the node attempt if a selected read or write value is missing.
- Validate each state write against the subschema for its destination path before applying the attempt’s writes. Reject all writes from the attempt if validation fails.
- Validate the payload assembled by a `SignalNode` against the event’s payload schema before signaling it.
- Omit `output_states` on duration and timestamp waits.

The ordering of concurrent writes to the same workflow state path remains to be defined.

## Workflow inputs

Callers can supply initial values for any attribute declared in `Workflow.states`, including attributes that nodes may write later. Use data flow analysis to identify required launch inputs. If a read can be reached along an execution path with no prior setter for that state path, mark the attribute as required in `Workflow.states`. For a required nested input, require each property along its path in the corresponding object schema.

Validate launch inputs against the root schema before initializing execution state. The policy for rechecking object-level constraints after state writes remains open.

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
