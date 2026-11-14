# Workflow execution

`WorkflowEngine` runs a saved [workflow revision](WORKFLOW.md#workflow) and checkpoints its progress in the database. The [node](NODES.md) and [edge](EDGES.md) specs define how execution moves through the graph.

## Components

### `WorkflowEngine`

`WorkflowEngine` orchestrates each `WorkflowExecution`. It determines which nodes should execute, manages timers and event waits, and dispatches node execution attempts. It owns the workflow’s shared `DataMap` and records progress as concurrent branches advance.

### `WorkflowExecutor`

`WorkflowExecutor` runs node execution attempts on a `Machine`. It registers with `WorkflowEngine`, runs the attempts the engine sends it, and reports their outcomes. The engine dispatches `ActionNode` and `DecisionNode` attempts to the executor, which calls their [Python entry point](WORKFLOW.md#executable-interface). The engine handles region traversal, commits signals, and resumes waits.

A `Machine` is a physical or virtual computer with its own execution environment, including installed software, environment variables, profiles, and secret references. Node execution attempts use the environment of their assigned machine.

Nodes within a workflow execution can share a machine or target different machines. The two default executor aliases are `local`, which resolves to the executor on the user’s machine, and `sandbox`, which resolves to the built-in sandbox executor.

#### Protocol

1. `WorkflowExecutor` is configured with the `WorkflowEngine` URL.
2. On first startup, the executor calls `RegisterExecutor` over gRPC at that URL. The engine returns an executor ID and token, and the executor saves both.
3. The executor opens a bidirectional `ConnectExecutor` call and sends its saved token in the gRPC call metadata.
4. The engine records each attempt before sending it to the selected executor. It sends the next attempt to that executor only after committing the current attempt’s outcome.
5. When the attempt finishes, the executor sends a `NodeExecutionResult` on the same call. The engine looks up the node execution, verifies that the authenticated executor is the one recorded for that attempt, and commits the checkpoint.
6. If the connection closes, the executor opens a new `ConnectExecutor` call with its saved token. How the executor confirms that a result was committed and reconciles pending results after reconnecting remains open.

Only one attempt for a node visit may be in flight at a time. The engine dispatches a retry only after the previous attempt has definitively finished and can no longer return a result. This rule also applies during recovery after a disconnect.

```mermaid
sequenceDiagram
    participant X as WorkflowExecutor
    participant E as WorkflowEngine
    Note over X: Configured with the engine URL
    opt First startup
        X->>E: RegisterExecutor
        E-->>X: Executor ID and token
    end
    X->>E: ConnectExecutor with saved token
    loop One attempt at a time
        Note over E: Record the node execution attempt
        E-->>X: NodeExecutionDispatch on the open stream
        Note over X: Run the node execution attempt
        X->>E: NodeExecutionResult on the open stream
        Note over E: Commit the checkpoint
    end
    opt Stream closes
        X->>E: ConnectExecutor with saved token
    end
```

##### APIs

###### `RegisterExecutor`

The engine creates an executor ID and token for a first-time registration.

```protobuf
rpc RegisterExecutor(RegisterExecutorRequest) returns (RegisterExecutorResponse)

RegisterExecutorRequest {}

RegisterExecutorResponse {
  executor_id: UUID
  token: str
}
```

###### `ConnectExecutor`

The engine streams `NodeExecutionDispatch` messages with node definitions and their inputs. The executor streams `NodeExecutionResult` messages back over the same call. In both messages, `node_xid` identifies the `node_execution` row.

```protobuf
rpc ConnectExecutor(stream NodeExecutionResult) returns (stream NodeExecutionDispatch)

NodeExecutionDispatch {
  node_xid: UUID
  node: Struct
  inputs: DataMap
}

NodeExecutionResult {
  node_xid: UUID
  status: ExecutionStatus
  outputs: optional DataMap
  started_at: datetime
  ended_at: datetime
}
```

The executor reports `COMPLETED` or `FAILED` for a finished attempt. In `NodeExecutionResult`, `started_at` and `ended_at` record when the executor started and finished the attempt.

Omit `outputs` for a failed attempt or when a completed node returns an empty `DataMap`. Otherwise, include the return value defined by the [executable interface](WORKFLOW.md#executable-interface).

## Lifecycle

The lifecycle describes how a `WorkflowExecution` moves from creation to completion or failure.

| `ExecutionStatus` | Meaning |
| --- | --- |
| `READY` | The execution has been created and is ready for the engine to begin. |
| `RUNNING` | The engine has begun and at least one path is active or can advance. |
| `WAITING` | Every remaining path is blocked on a timer or event, directly or through a join. |
| `FAILED` | An unrecoverable error ended the execution. |
| `COMPLETED` | The root region completed under the applicable iteration, join, and merge rules. |

Set `started_at` when creating the execution in `READY`. Set `ended_at` when it enters `FAILED` or `COMPLETED`. A timer or event can move a waiting execution back to `RUNNING`.

## Persistence model

`WorkflowEngine` stores workflow executions and their node executions in the database using the tables below.

### `workflow_execution`

| Column | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Primary key for the `WorkflowExecution`. |
| `workflow_id` | `UUID` | Identifies the source workflow. |
| `workflow_revision` | `int` | Identifies the saved revision. |
| `workflow_snapshot` | JSON | Stores an immutable copy of the `Workflow` used by the execution. |
| `status` | `ExecutionStatus` | Stores the workflow execution status. |
| `started_at` | `datetime` | Records when the execution was created. |
| `ended_at` | `datetime` (nullable) | Records when the execution ended. |
| `states` | JSON | Stores the current [DataMap](NODES.md#datamap), initialized from the validated [workflow inputs](WORKFLOW.md#workflow-inputs). |

### `node_execution`

| Column | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Primary key for one visit to the node. |
| `workflow_xid` | `UUID` | Foreign key to `workflow_execution.id`. |
| `node_id` | `UUID` | Identifies the node in the workflow snapshot. |
| `status` | `ExecutionStatus` | Stores the node execution status. |
| `executor_id` | `UUID` (nullable) | Identifies the `WorkflowExecutor` assigned to the current attempt. |
| `attempt_count` | `int` | Starts at zero before dispatch, becomes one on first dispatch, and increments on each retry. |
| `started_at` | `datetime` | Records when the node visit was created. |
| `attempted_at` | `datetime` (nullable) | Records when the latest attempt began. |
| `ended_at` | `datetime` (nullable) | Records when the node visit ended. |

A loop or a later traversal to the same `node_id` creates a new row. A retry updates the existing row and keeps its `started_at`.

## Dispatch and checkpoints

When a node becomes ready to run, `WorkflowEngine` creates a `node_execution` row in `READY`. Before dispatching the node to an executor, the engine updates that row to `RUNNING` and records `attempt_count` and the selected executor.

After each completed step, the engine records the node status, applies the node’s [state writes](WORKFLOW.md#node-state-bindings), records the next execution positions, and updates the parent status in one database transaction. A [Fork](EDGES.md#fork) creates its branch entry records in the same checkpoint. Concurrent branches can run independently. The engine serializes commits that update the same workflow execution and applies each accepted attempt’s state writes once.

## Recovery and open decisions

- Restore runnable node visits and workflow state from the last committed checkpoint. Recovery must preserve the [single-attempt rule](#protocol). The mechanisms for confirming committed results and reconciling pending results over the stream remain open, as does the policy for retrying actions with external side effects.
- Derive each wait’s configuration from the snapshotted `WaitNode`. The engine still needs a durable way to preserve timer deadlines and event delivery progress across restarts. The representation remains open.
- Keep join arrivals scoped to the same [Fork activation](EDGES.md#join). The representation for reconstructing activation membership and pending arrivals after a restart remains open, including for nested Forks and loops.
- Define the ordering of concurrent writes to overlapping state paths in the [state binding spec](WORKFLOW.md#state-paths).
- Defer persisted node results and workflow or node failure details.
