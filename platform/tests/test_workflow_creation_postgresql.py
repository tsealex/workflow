import asyncio
import os
import unittest
from datetime import UTC, datetime
from uuid import uuid4

from jsonschema.exceptions import ValidationError
from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from engine import WorkflowEngine
from models import (
    ExecutionStatus,
    Workflow,
    WorkflowExecution,
    WorkflowSnapshot,
)
from nodes import RegionNode


@unittest.skipUnless(
    os.environ.get("WORKFLOW_TEST_POSTGRES_URL"),
    "Set WORKFLOW_TEST_POSTGRES_URL to a migrated PostgreSQL test database.",
)
class WorkflowCreationPostgreSQLTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        database = create_async_engine(os.environ["WORKFLOW_TEST_POSTGRES_URL"])
        self.addAsyncCleanup(database.dispose)
        self.sessions = async_sessionmaker(database)
        self.engine = WorkflowEngine(self.sessions)
        self.workflow_id = uuid4()
        self.addAsyncCleanup(self.delete_workflow_records)

        # Use a synthetic workflow with a required nested input.
        root = RegionNode(id=uuid4(), name="Test workflow", nodes=[], edges=[])
        schema = {
            "type": "object",
            "properties": {
                "item": {
                    "type": "object",
                    "properties": {"quantity": {"type": "integer"}},
                    "required": ["quantity"],
                }
            },
            "required": ["item"],
        }
        async with self.sessions.begin() as session:
            session.add(
                Workflow(
                    id=self.workflow_id,
                    root_json=root.model_dump(mode="json"),
                    states=schema,
                    revision=1,
                )
            )

    async def delete_workflow_records(self):
        async with self.sessions.begin() as session:
            await session.execute(
                delete(WorkflowExecution).where(
                    WorkflowExecution.workflow_id == self.workflow_id
                )
            )
            await session.execute(
                delete(WorkflowSnapshot).where(
                    WorkflowSnapshot.workflow_id == self.workflow_id
                )
            )
            await session.execute(
                delete(Workflow).where(Workflow.id == self.workflow_id)
            )

    async def snapshot_count(self):
        async with self.sessions() as session:
            return await session.scalar(
                select(WorkflowSnapshot.ref_count).where(
                    WorkflowSnapshot.workflow_id == self.workflow_id,
                    WorkflowSnapshot.revision == 1,
                )
            )

    async def test_concurrent_starts_share_snapshot_and_cache_execution(self):
        inputs = {"item": {"quantity": 2}}
        other_engine = WorkflowEngine(self.sessions)
        first, second = await asyncio.gather(
            self.engine.create_workflow_execution(self.workflow_id, inputs),
            other_engine.create_workflow_execution(self.workflow_id, inputs),
        )
        self.assertNotEqual(first, second)
        self.assertEqual(await self.snapshot_count(), 2)
        snapshot_id = (self.workflow_id, 1)
        snapshots = [
            engine._workflow_snapshots[snapshot_id]
            for engine in (self.engine, other_engine)
        ]
        self.assertEqual(
            sorted(snapshot.ref_count for snapshot in snapshots), [1, 2]
        )
        inputs["item"]["quantity"] = 3
        cached = self.engine._executions[first]
        self.assertEqual(cached.workflow.states, {"item": {"quantity": 2}})
        self.assertEqual(cached.nodes, {})

        async with self.sessions() as session:
            execution = await session.get(WorkflowExecution, first)
            snapshot = await session.get(
                WorkflowSnapshot, (self.workflow_id, 1)
            )
            workflow = await session.get(Workflow, self.workflow_id)
            self.assertEqual(execution.status, ExecutionStatus.RUNNING)
            self.assertEqual(execution.workflow_revision, 1)
            self.assertEqual(execution.states, cached.workflow.states)
            self.assertIsNone(execution.ended_at)
            self.assertEqual(snapshot.root, workflow.root_json)
            self.assertEqual(snapshot.states, workflow.states)

    async def test_invalid_start_creates_no_snapshot_or_cache(self):
        with self.assertRaises(ValidationError):
            await self.engine.create_workflow_execution(self.workflow_id, {})
        with self.assertRaises(LookupError):
            await self.engine.create_workflow_execution(uuid4(), {})
        self.assertIsNone(await self.snapshot_count())
        self.assertEqual(self.engine._executions, {})
        self.assertEqual(self.engine._workflow_snapshots, {})

    async def test_workflow_status_update_releases_snapshot_on_commit(self):
        xid = await self.engine.create_workflow_execution(
            self.workflow_id, {"item": {"quantity": 2}}
        )
        async with self.sessions() as session:
            await self.engine.update_workflow_execution_status(
                session, xid, ExecutionStatus.FAILED
            )
            await session.flush()
            await session.rollback()
        self.assertEqual(await self.snapshot_count(), 1)
        self.assertIn(xid, self.engine._executions)

        async with self.sessions.begin() as session:
            await self.engine.update_workflow_execution_status(
                session, xid, ExecutionStatus.COMPLETED
            )
            self.assertIn(xid, self.engine._executions)
        self.assertEqual(await self.snapshot_count(), 0)
        self.assertNotIn(xid, self.engine._executions)

    async def test_trigger_counts_active_transitions_deletes_and_rollback(self):
        inputs = {"item": {"quantity": 2}}
        xid = await self.engine.create_workflow_execution(
            self.workflow_id, inputs
        )
        self.assertEqual(await self.snapshot_count(), 1)

        for status, expected in (
            (ExecutionStatus.SUSPENDED, 1),
            (ExecutionStatus.RUNNING, 1),
            (ExecutionStatus.COMPLETED, 0),
            (ExecutionStatus.COMPLETED, 0),
            (ExecutionStatus.FAILED, 0),
            (ExecutionStatus.RUNNING, 1),
        ):
            with self.subTest(status=status):
                async with self.sessions.begin() as session:
                    await session.execute(
                        update(WorkflowExecution)
                        .where(WorkflowExecution.xid == xid)
                        .values(status=status)
                    )
                self.assertEqual(await self.snapshot_count(), expected)

        async with self.sessions() as session:
            await session.execute(
                delete(WorkflowExecution).where(WorkflowExecution.xid == xid)
            )
            await session.rollback()
        self.assertEqual(await self.snapshot_count(), 1)

        async with self.sessions.begin() as session:
            await session.execute(
                delete(WorkflowExecution).where(WorkflowExecution.xid == xid)
            )
        self.assertEqual(await self.snapshot_count(), 0)

        async with self.sessions.begin() as session:
            terminal = WorkflowExecution(
                xid=uuid4(),
                workflow_id=self.workflow_id,
                workflow_revision=1,
                status=ExecutionStatus.FAILED,
                started_at=datetime.now(UTC),
                states=inputs,
            )
            session.add(terminal)
            await session.flush()
            await session.delete(terminal)
        self.assertEqual(await self.snapshot_count(), 0)

    async def test_activating_execution_requires_a_snapshot(self):
        xid = uuid4()
        async with self.sessions.begin() as session:
            session.add(
                WorkflowExecution(
                    xid=xid,
                    workflow_id=self.workflow_id,
                    workflow_revision=1,
                    status=ExecutionStatus.FAILED,
                    started_at=datetime.now(UTC),
                    states={"item": {"quantity": 2}},
                )
            )

        with self.assertRaises(IntegrityError) as error:
            async with self.sessions.begin() as session:
                await session.execute(
                    update(WorkflowExecution)
                    .where(WorkflowExecution.xid == xid)
                    .values(status=ExecutionStatus.RUNNING)
                )
        self.assertEqual(error.exception.orig.sqlstate, "23503")
        async with self.sessions() as session:
            execution = await session.get(WorkflowExecution, xid)
            self.assertEqual(execution.status, ExecutionStatus.FAILED)
