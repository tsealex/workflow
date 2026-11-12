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

Execution starts at the root region's `initial` point and completes at its `final` point. Each run holds its own state values.

Each node belongs to this workflow. Its saved changes are tracked by the workflow revision.

## Node state bindings

The following attributes extend node instances within a workflow, including nodes in nested regions. Their base attributes are defined in [Workflow nodes](NODES.md).

### `ActionNode`

Reads workflow states into its inputs and writes its outputs to workflow states.

| Attribute | Type | Description |
| --- | --- | --- |
| `input_states` | [StateMap](#statemap) | Maps input attributes to states to read. |
| `output_states` | [StateMap](#statemap) | Maps output attributes to states to write. |

### `DecisionNode`

Reads workflow states into its inputs and selects a branch.

| Attribute | Type | Description |
| --- | --- | --- |
| `input_states` | [StateMap](#statemap) | Maps input attributes to states to read. |

The selected branch name determines the outgoing edge.

### `SignalNode`

Reads workflow states to populate the event payload.

See [Events](EVENTS.md) for workspace scope and how a signal reaches matching waits.

| Attribute | Type | Description |
| --- | --- | --- |
| `input_states` | [StateMap](#statemap) | Maps event payload attributes to states to read. |

### `WaitNode`

When configured with `event`, writes received payload attributes to workflow states.

| Attribute | Type | Description |
| --- | --- | --- |
| `output_states` | [StateMap](#statemap) (optional) | Maps received payload attributes to states to write. |

## Types

### `StateMap`

A map from node attribute names to workflow state names. Keys and values support dotted paths for nested attributes.

```json
{
  "amount": "order.total",
  "address.city": "customer.address.city"
}
```

For `input_states`, the node reads the state path into the named attribute. For `output_states`, it writes the attribute's value to the state path. An empty map binds no attributes.

Each mapping must reference declared node attributes and workflow states with compatible types. Mappings contain only names or paths. Calculations belong in the node's logic.
