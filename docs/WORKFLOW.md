# Workflow

A workflow embeds a root region and defines the states used by its nodes.

## `Workflow`

| Attribute | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Uniquely identifies the workflow. |
| `root` | [RegionNode](NODES.md#regionnode) | Contains the workflow's nodes and edges. |
| `states` | [TypeMap](NODES.md#typemap) | Defines workflow state names and their types. |
| `revision` | `int` | Increments whenever node changes are saved, including changes within nested regions. |
| `name` | `str` (optional) | Names the workflow. |
| `description` | `str` (optional) | Describes the workflow's purpose. |

A workflow execution starts at the root region's `initial` node and completes at its `final` node. Each workflow execution holds its own state values. See [Workflow execution](EXECUTION.md) for lifecycle and persistence.

Each node belongs to this workflow. Its saved changes are tracked by the workflow revision.

## Edges

See [Edges](EDGES.md) for edge definitions, region connections, and execution patterns.

## Node state bindings

The following attributes extend node instances within a workflow, including nodes in nested regions. Their base attributes are defined in [Workflow nodes](NODES.md).

### `ActionNode`

Reads workflow states into its inputs and writes its outputs to workflow states.

| Attribute | Type | Description |
| --- | --- | --- |
| `code` | `str` | Stores the [Python code](#executable-interface) an LLM generates from `instruction` to run the action. |
| `input_states` | [StateMap](#statemap) | Maps input attributes to workflow state paths to read. |
| `output_states` | [StateMap](#statemap) | Maps output attributes to workflow state paths to write. |

### `DecisionNode`

Reads workflow states into its inputs and selects a branch.

| Attribute | Type | Description |
| --- | --- | --- |
| `code` | `str` | Stores the [Python code](#executable-interface) an LLM generates from `instruction` to select a branch. |
| `input_states` | [StateMap](#statemap) | Maps input attributes to workflow state paths to read. |

The `branch` attribute in `outputs` determines the [outgoing edge](EDGES.md#branch-edge).

### `SignalNode`

Reads workflow states to populate the event payload.

See [Events](EVENTS.md) for how a `WaitNode` receives a matching event instance in the same workspace.

| Attribute | Type | Description |
| --- | --- | --- |
| `input_states` | [StateMap](#statemap) | Maps event payload attributes to workflow state paths to read. |

### `WaitNode`

When configured with `event`, writes received payload attributes to workflow states.

| Attribute | Type | Description |
| --- | --- | --- |
| `output_states` | [StateMap](#statemap) (optional) | Maps received payload attributes to workflow state paths to write. |

## Executable interface

When compiling a node’s `instruction`, the LLM generates a concrete Python subclass of `Executable` with both methods below. The workflow stores the generated class in the node’s `code` field. `WorkflowExecutor` calls `execute` for each node execution attempt. The validation harness calls `test_execute` during compilation.

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

- `inputs` contains the node’s input attributes after the engine resolves `input_states`. The attribute names and value types follow the node’s `inputs` declaration.
- An `ActionNode` returns the output attributes declared by the node. Return an empty [DataMap](NODES.md#datamap) if the action has no outputs. The engine applies `output_states` to update workflow state.
- A `DecisionNode` returns `{"branch": label}`, where `label` is a declared branch or `"default"`. The engine uses that label to select the outgoing edge.
- The executor reports the attempt as failed if `execute` raises an exception or returns a value that violates the node’s output contract.

### Code validation

The generated `test_execute` calls `execute` with representative inputs and asserts the expected outputs. It can temporarily replace external dependencies with test doubles. The validation harness runs `test_execute` in an isolated environment without live credentials and treats an exception as a validation failure.

## Types

### `StateMap`

A map from node attribute paths to workflow state paths. Use dotted paths for nested attributes or states.

```json
{
  "amount": "order.total",
  "address.city": "customer.address.city"
}
```

For `input_states`, the node reads the state path into the named attribute. For `output_states`, it writes the attribute's value to the state path. An empty map binds no attributes.

Each mapping must reference declared node attributes and workflow states with compatible types. Mappings contain only names or paths. Calculations belong in the node's logic.

The ordering of concurrent writes to the same workflow state path remains to be defined.
