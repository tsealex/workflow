import asyncio
import unittest
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

import execution_pb2 as messages
from executors import WorkflowExecutor, _NodeAttempt


class ExecutorResultTest(unittest.IsolatedAsyncioTestCase):
    async def test_results_keep_workflow_execution_id_for_each_outcome(self):
        for status in (
            messages.COMPLETED,
            messages.FAILED,
            messages.INTERRUPTED,
        ):
            with self.subTest(status=status):
                workflow_xid = str(uuid4())
                node_xid = str(uuid4())
                request = messages.ExecuteNodeRequest(
                    workflow_xid=workflow_xid,
                    node_xid=node_xid,
                )
                request = messages.ExecuteNodeRequest.FromString(
                    request.SerializeToString()
                )

                async def execute():
                    if status == messages.FAILED:
                        raise ValueError("Synthetic execution failure")
                    if status == messages.INTERRUPTED:
                        raise asyncio.CancelledError
                    return {}

                execution = asyncio.create_task(execute())
                await asyncio.gather(execution, return_exceptions=True)
                started_at = datetime.now(UTC)
                attempt = _NodeAttempt(request, started_at, execution)
                with patch("executors.logger.exception"):
                    result = WorkflowExecutor._node_result(attempt)
                result = messages.ExecuteNodeResponse.FromString(
                    result.SerializeToString()
                )
                self.assertEqual(result.workflow_xid, workflow_xid)
                self.assertEqual(result.node_xid, node_xid)
                self.assertEqual(result.status, status)
