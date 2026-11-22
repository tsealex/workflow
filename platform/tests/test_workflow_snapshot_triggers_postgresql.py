import os
import unittest
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from database import AsyncSession
from models import ExecutionStatus, WorkflowExecution, WorkflowSnapshot


@unittest.skipUnless(
    os.environ.get("WORKFLOW_TEST_POSTGRES_URL"),
    "Set WORKFLOW_TEST_POSTGRES_URL to a migrated PostgreSQL test database.",
)
class WorkflowSnapshotTriggerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        database = create_async_engine(os.environ["WORKFLOW_TEST_POSTGRES_URL"])
        self.addAsyncCleanup(database.dispose)
        self.sessions = async_sessionmaker(database, class_=AsyncSession)
        self.workflow_id = uuid4()
        self.addAsyncCleanup(self.delete_workflow_executions)

    async def delete_workflow_executions(self):
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

    async def snapshot_count(self):
        async with self.sessions() as session:
            return await session.scalar(
                select(WorkflowSnapshot.ref_count).where(
                    WorkflowSnapshot.workflow_id == self.workflow_id,
                    WorkflowSnapshot.revision == 1,
                )
            )

    async def test_trigger_counts_active_transitions_deletes_and_rollback(self):
        inputs = {"item": {"quantity": 2}}
        xid = uuid4()
        started_at = datetime.now(UTC)
        snapshot = WorkflowSnapshot(
            workflow_id=self.workflow_id,
            revision=1,
            root={},
            states={},
        )
        execution = WorkflowExecution(
            xid=xid,
            workflow_id=self.workflow_id,
            workflow_revision=1,
            status=ExecutionStatus.RUNNING,
            started_at=started_at,
            states=inputs,
        )
        async with self.sessions.begin() as session:
            session.add(snapshot)
            await session.flush()
            session.add(execution)
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
