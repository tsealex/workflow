# Workflow execution

`WorkflowEngine` runs a saved [WorkflowSnapshot](#workflow_snapshot) and checkpoints its progress in the database. The [node](NODES.md) and [edge](EDGES.md) specs define how execution moves through the graph.

## Components

### `WorkflowEngine`

`WorkflowEngine` orchestrates each `WorkflowExecution`. It determines which nodes should execute, manages timers and event waits, and dispatches node executions. It owns the workflow’s shared `DataMap` and records progress as concurrent branches advance.

### `WorkflowExecutor`

`WorkflowExecutor` runs node executions on a `Machine`. It registers with `WorkflowEngine`, runs the nodes the engine dispatches, and reports their outcomes. The engine dispatches `ActionNode` and `DecisionNode` executions to the executor, which calls their [Python entry point](WORKFLOW.md#executable-interface). The engine exclusively handles region executions, processes collection items sequentially, commits signals, and advances execution when waits end.

A `Machine` is a physical or virtual computer with its own execution environment, including installed software, environment variables, profiles, and secret references. Node executions use the environment of their assigned machine.

Nodes within a workflow execution can share a machine or target different machines. The two default executor aliases are `local`, which resolves to the executor on the user’s machine, and `sandbox`, which resolves to the built-in sandbox executor.

#### Protocol

1. `WorkflowExecutor` is configured with the `WorkflowEngine` URL.
2. On startup, the executor calls [`RegisterExecutor`](#registerexecutor) with saved credentials, or saves newly issued credentials on first registration.
3. The executor opens `ConnectExecutor` with its token in gRPC metadata. The engine records `last_connected_at` after authentication.
4. The engine records the node execution before sending an `ExecuteNodeRequest` to the selected executor. It sends the next node to that executor only after committing the current node's result.
5. The executor sends a `ExecuteNodeResponse` when the node finishes or stops in response to an interruption. The engine verifies that the sender is the assigned executor before committing the result.
6. The engine can send an `InterruptNodeRequest` while a node is running. The executor must be able to receive and process this message during execution.
7. The executor reconnects with its saved token after restart. A disconnect follows the [executor shutdown rules](#executor-shutdown).

Only one execution of a given node visit may run at a time. A failed node visit and its workflow remain `FAILED`.

```mermaid
sequenceDiagram
    participant X as WorkflowExecutor
    participant E as WorkflowEngine
    Note over X: Configured with the engine URL
    X->>E: RegisterExecutor with saved credentials, or neither field
    E-->>X: Authenticated or newly created executor ID and token
    X->>E: ConnectExecutor with saved token
    loop One node at a time
        Note over E: Record the node execution
        E-->>X: ExecuteNodeRequest
        Note over X: Run the node
        opt Engine requests interruption
            E-->>X: InterruptNodeRequest
            Note over X: Stop the running node
        end
        X->>E: ExecuteNodeResponse
        Note over E: Commit the checkpoint
    end
    opt Stream closes
        X->>E: ConnectExecutor with saved token
    end
```

##### APIs

###### `RegisterExecutor`

The engine authenticates the supplied ID and token against [`workflow_executor`](#workflow_executor), or creates a registration when both are omitted. The engine rejects incomplete or invalid credentials and IDs that are already connected. Each ID can have one live stream.

The executor saves credentials in `~/.workflow/executor.credentials`, encrypted with a key in the user's OS keyring, and loads them on startup. The executor reports unreadable or invalid credentials as errors.

```protobuf
rpc RegisterExecutor(RegisterExecutorRequest) returns (RegisterExecutorResponse)

RegisterExecutorRequest {
  executor_id: optional UUID
  token: optional str
}

RegisterExecutorResponse {
  executor_id: UUID
  token: str
}
```

###### `ConnectExecutor`

The executor calls `ConnectExecutor` on the engine to open the stream. The engine sends `EngineMessage` envelopes over that stream, and the executor sends `ExecutorMessage` envelopes. Each envelope contains one typed payload. `node_xid` identifies the `node_execution` row.

```protobuf
rpc ConnectExecutor(stream ExecutorMessage) returns (stream EngineMessage)

EngineMessage {
  oneof payload {
    ExecuteNodeRequest execute_node
    InterruptNodeRequest interrupt_node
    EngineShutdown shutdown
  }
}

ExecutorMessage {
  oneof payload {
    ExecuteNodeResponse node_result
    ExecutorShutdown shutdown
  }
}

ExecuteNodeRequest {
  node_xid: UUID
  started_at: datetime
  node: Struct
  inputs: optional DataMap
}

InterruptNodeRequest {
  node_xid: UUID
}

ExecuteNodeResponse {
  node_xid: UUID
  status: ExecutionStatus
  outputs: optional DataMap
  started_at: datetime
  ended_at: datetime
}

EngineShutdown {}

ExecutorShutdown {}
```

The executor reports `COMPLETED` when the node succeeds, `FAILED` when it encounters an unexpected error.

In `ExecuteNodeResponse`, `started_at` and `ended_at` record when the executor started and stopped running the node. A suspended result's `ended_at` records that stop and does not set the database row's terminal `ended_at`.

Include `outputs` only for a completed node with a nonempty return value, as defined by the [executable interface](WORKFLOW.md#executable-interface).

## Lifecycle

The lifecycle describes how a `WorkflowExecution` moves from creation to completion or failure.

| `ExecutionStatus` | Meaning |
| --- | --- |
| `RUNNING` | Execution is active, including while awaiting dispatch, a timer, an event, or a join. |
| `FAILED` | A node failed, ending the workflow. |
| `SUSPENDED` | Execution is paused and can continue from its saved progress. |
| `INTERRUPTED` | A node execution was terminated by interruption. The node visit is terminal and cannot resume. This status applies to node executions only. |
| `COMPLETED` | The root region completed under the applicable iteration, join, and merge rules. |

If a node fails, the engine marks that node and its workflow `FAILED` and marks the workflow's other unfinished node executions `INTERRUPTED`. It stops scheduling work for that workflow and interrupts its other running nodes. Results received after the workflow has failed are ignored.

A workflow remains `RUNNING` while any of its nodes is running. It becomes `SUSPENDED` when no nodes are running and none of its suspended nodes can be scheduled. Engine shutdown follows the [shutdown steps](#engine-shutdown).

The engine creates executions in `RUNNING` and sets `started_at` at creation. It sets `ended_at` when an execution enters `FAILED`, `COMPLETED`, or `INTERRUPTED`. Waiting does not change the execution's status.

## Persistence model

`WorkflowEngine` stores executor registrations, workflow executions, and node executions in the database using the tables below.

Each execution identifies its shared [WorkflowSnapshot](#workflow_snapshot) by `workflow_id` and `workflow_revision`.

### `workflow_executor`

The engine persists each executor registration so its ID and affinity bindings survive restarts. The executor stores the returned ID and raw token in an encrypted local file and loads them on startup. The engine stores the token hash for authentication.

| Column | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Identifies the executor and remains stable across restarts. |
| `token_hash` | `str` | Stores the hash used to authenticate the executor token. |
| `created_at` | `datetime` | Records when the engine created the registration. |
| `last_connected_at` | `datetime` (nullable) | Records the latest authenticated stream connection, or null before the first connection. |

### `workflow_snapshot`

The `workflow_snapshot` table stores a shared, immutable graph and state schema per revision, avoiding duplicate storage and preserving execution behavior across edits and restarts.

When starting a workflow execution, `WorkflowEngine` creates a snapshot of the current revision if one does not already exist.

| Column | Type | Description |
| --- | --- | --- |
| `workflow_id` | `UUID` | Identifies the workflow and forms the composite primary key with `revision`. |
| `revision` | `int` | Identifies the saved workflow revision. |
| `root` | JSON | Stores the serialized [RegionNode](NODES.md#regionnode). |
| `states` | [JSONSchema](WORKFLOW.md#jsonschema) | Stores the workflow state schema as JSON. |
| `ref_count` | `int` | Counts unfinished workflow executions using the snapshot. |

Each `RUNNING` or `SUSPENDED` execution holds one reference. The engine increments `ref_count` at execution creation and decrements it once on `COMPLETED` or `FAILED`. Periodic cleanup deletes snapshots with `ref_count = 0`, removing their historical graph and schema.

### `workflow_execution`

| Column | Type | Description |
| --- | --- | --- |
| `id` | `UUID` | Primary key for the `WorkflowExecution`. |
| `workflow_id` | `UUID` | Identifies the source workflow. |
| `workflow_revision` | `int` | Identifies the shared snapshot's `revision` together with `workflow_id`. |
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
| `inputs` | JSON (nullable) | Stores a fixed copy of the inputs saved before execution starts, including selected collection item values. Null until saved. An empty input object is stored as `{}`. Resuming a suspended visit uses these saved inputs. |
| `executor_id` | `UUID` (nullable) | Identifies the `WorkflowExecutor` assigned to the node execution. |
| `started_at` | `datetime` | Records when the node visit was created. |
| `ended_at` | `datetime` (nullable) | Records when the node visit ended. |

Each node visit has its own row. This includes concurrent visits on fork branches and successive visits in collection iterations. A loop or a later traversal to the same `node_id` creates a new row. Resuming a suspended visit uses the existing row and preserves its `started_at`.

## Region execution

`WorkflowEngine` processes a region's collection one item at a time. For each item, execution begins at `initial`. The engine starts the next item only after execution for the current item reaches `final`. The region completes normally after every item finishes. Nested collection regions follow the same rule.

An interruptible edge that exits the region ends its current iteration and prevents the remaining items from starting. Execution continues at the edge's target according to the [interruptible edge rules](EDGES.md#interruptible-edge).

## Fork, join, and merge scheduling

`WorkflowEngine` tracks branches separately for each fork execution. This algorithm requires an assigned join for each fork. The examples below specify those assignments.

The engine follows the selected edge from a `DecisionNode` and all outgoing edges from a non-decision node, following the [edge patterns](EDGES.md#edge-patterns).

```text
When an execution completes successfully:
    Identify its selected successors.
    If it selects one successor:
        Continue the same branch to that successor.
    If it selects multiple successors:
        Remember the join assigned to this fork.
        Track one pending branch per selected successor.
        Remember the enclosing branch, if any.
        Start each selected successor independently.

When a branch reaches a node:
    If this node is the join assigned to that branch:
        Mark the branch as arrived.
        Wait for all branches from the same fork execution.
        Once all have arrived:
            If an enclosing branch exists:
                Continue that branch to this node and repeat the check.
            Otherwise:
                Start one execution of the join.
    Otherwise:
        Start a new execution of this node on the same branch.
```

The engine keeps separate [node visits](#node_execution) for concurrent executions and loop iterations. The [checkpoint rules](#dispatch-and-checkpoints) apply to each transition. Subscripts in the examples identify separate visits, such as `B₁` and `B₂`.

### Decision followed by a merge

`A` selects exactly one of `B` and `C`. Both paths lead to `D`.

```mermaid
flowchart LR
    A{A} -->|b| B[B]
    A -->|default| C[C]
    B --> D[D]
    C --> D
```

`A` selects `B`, so the engine starts `B₁`. The engine starts `D₁` when `B₁` finishes because no enclosing fork waits at `D`. Selecting `C` follows the same sequence through `C`.

### Fork followed by repeated joins

The fork at `A` joins at `B`. Each execution of `B` forks again and joins at the next execution of `B`.

```mermaid
flowchart LR
    A[A] --> G[G]
    A --> D[D]
    G --> B[B]
    D --> B
    B --> C[C]
    C --> B
    B --> E[E]
    E --> D
```

1. `A` finishes, and the engine starts `G₁` and `D₁`.
2. The engine starts `B₁` after both finish.
3. `B₁` finishes, and the engine starts `C₁` and `E₁`.
4. `E₁` finishes, and the engine starts `D₂` on the same branch.
5. The engine starts `B₂` after both `C₁` and `D₂` finish, in either order.

The completion of `D₁` belongs to the fork started by `A`. The engine waits for `D₂` to complete the branch started by `B₁`.

### Concurrent visits to the same node

`S` starts both `A` and `B`, and its branches join at `F`. The decision nodes `A` and `B` can select each other or exit to `F`.

```mermaid
flowchart LR
    S[S] --> A{A}
    S --> B{B}
    A -->|repeat| B
    B -->|repeat| A
    A -->|default| F[F]
    B -->|default| F
```

1. `S` finishes, and the engine starts `A₁` and `B₁` concurrently.
2. `A₁` selects `B`, so the engine starts `B₂` even if `B₁` is still running.
3. `B₁` selects `A`, so the engine starts `A₂`.
4. `B₂` and `A₂` select `F`, and the engine executes `F` once both arrive.

The engine waits at `F` because it is the assigned join. Each visit to `A` or `B` starts a separate execution on its existing branch.

## Node inputs and outputs

The engine validates workflow inputs against `Workflow.states` before initializing workflow state.

### Input copies

Before a node visit starts, the engine saves a copy of its selected inputs in [`node_execution.inputs`](#node_execution). This includes values selected from the current collection item or an enclosing region's current item.

Each node visit has its own saved inputs, including visits that run concurrently. The node receives a copy of its saved inputs, including when execution continues from `SUSPENDED`. Later changes to workflow state do not change these inputs.

For `ActionNode` and `DecisionNode`, the executor passes that copy in `ExecContext.input` when it calls [`Executable.execute`](WORKFLOW.md#executable-interface).

### Output application

The engine applies completed nodes' outputs one at a time, in the order it receives their completion results. The engine applies all outputs from each node together.

When outputs overlap at the same state path, the output applied later takes precedence. For example, if two nodes output `payment.approved`, the result received second determines its final value. An output for `payment` also replaces its child values.

### Runtime validation

The engine validates inputs before execution, outputs before applying them, and signal payloads before committing events. Each value must match its state, item, or event schema.

Missing values, validation errors, or execution exceptions fail the node and its workflow. The engine rejects all outputs from a failed node.

## Dispatch and checkpoints

When a node becomes eligible to run, `WorkflowEngine` creates a `node_execution` row in `RUNNING`. Before dispatch, it saves inputs according to [Node inputs and outputs](#node-inputs-and-outputs) and records the selected executor. During suspension, it records successors as `SUSPENDED` without starting them; their inputs remain null until they start.

The engine commits each execution transition atomically, including completion, failure, interruption, and suspension.

After each completed step, the engine records the node status, applies the node’s [state writes](WORKFLOW.md#node-state-bindings), records the next execution positions, and updates the parent status. A [Fork](EDGES.md#fork) creates its branch entry records as part of that transition. Concurrent branches can run independently. The engine serializes commits that update the same workflow execution and applies each completed node's state writes once.

## Checkpoint and restart

A normal shutdown saves unfinished work so execution can continue after restart. `WorkflowEngine` owns this process, including suspension of running nodes.

### Engine shutdown

1. Stop accepting new workflow executions and stop scheduling new nodes.
2. Allow running nodes to finish and process their results during the configured shutdown grace period (1 minute by default).
3. After grace period, mark all `RUNNING` workflows and node executions `SUSPENDED`. Ignore `ExecuteNodeResponse` received for suspended nodes.
4. Send each executor `EngineShutdown` and close its connection without waiting for a response.
5. After all executors disconnect, the engine shuts down.

### Executor interruption

When execution follows an interruptible edge, the engine marks the affected unfinished node executions in the exited regions `INTERRUPTED`, cancels their pending joins and waits, and records the edge's target execution position. After committing, the engine sends `InterruptNodeRequest` to executors running those nodes.

The engine ignores results for `INTERRUPTED` node executions. Outputs committed before the interruption remain in workflow state.

The executor handles interruption messages as follows:

- `InterruptNodeRequest`: Stop the node execution identified by `node_xid` if it is still running.
- `EngineShutdown`: Stop all ongoing node executions.

### Executor shutdown

1. The executor stops all ongoing node executions.
2. The executor sends `ExecutorShutdown` and shuts down without waiting for a response.
3. On receiving `ExecutorShutdown`, the engine stops assigning work to that executor and marks its running nodes `SUSPENDED`.
4. The engine tries to reschedule those nodes on eligible executors, respecting executor bindings.
5. Whenever another node in an affected workflow completes, the engine checks its suspended nodes for scheduling again.
6. The engine marks the workflow `SUSPENDED` only when no nodes are running and none can be scheduled.

If the executor disconnects without sending `ExecutorShutdown`, the engine performs the same suspension and rescheduling steps when it detects the disconnection. 

### Engine restart

1. Mark workflows and node executions left in `RUNNING` as `SUSPENDED`.
2. Schedule suspended nodes when eligible executors are available.
3. Mark each resumed node and its workflow `RUNNING`.

