import unittest
from datetime import timedelta
from uuid import uuid4

from models import WorkflowSnapshot
from nodes import RegionNode, WaitNode


class WorkflowSnapshotTest(unittest.TestCase):
    """Check region lookups in a synthetic workflow hierarchy."""

    def setUp(self):
        node_id = uuid4()
        inner_id = uuid4()
        outer_id = uuid4()
        sibling_id = uuid4()
        root_id = uuid4()
        workflow_id = uuid4()
        duration = timedelta(seconds=1)
        self.node = WaitNode(id=node_id, name="Wait", duration=duration)
        self.inner = RegionNode(
            id=inner_id, name="Inner", nodes=[self.node], edges=[]
        )
        self.outer = RegionNode(
            id=outer_id, name="Outer", nodes=[self.inner], edges=[]
        )
        self.sibling = RegionNode(
            id=sibling_id, name="Sibling", nodes=[], edges=[]
        )
        self.root = RegionNode(
            id=root_id,
            name="Root",
            nodes=[self.outer, self.sibling],
            edges=[],
        )
        root_json = self.root.model_dump(mode="json")
        self.snapshot = WorkflowSnapshot(
            workflow_id=workflow_id,
            revision=1,
            root=root_json,
            states={"type": "object"},
        )

    def test_get_region_id_returns_immediate_enclosing_region(self):
        self.assertEqual(
            self.snapshot.get_region_id(self.node.id), self.inner.id
        )
        self.assertEqual(
            self.snapshot.get_region_id(self.inner.id), self.outer.id
        )
        self.assertEqual(
            self.snapshot.get_region_id(self.outer.id), self.root.id
        )
        with self.assertRaises(KeyError):
            self.snapshot.get_region_id(self.root.id)

    def test_get_nested_region_ids_returns_direct_regions(self):
        self.assertEqual(
            self.snapshot.get_nested_region_ids(self.root.id),
            [self.outer.id, self.sibling.id],
        )
        self.assertEqual(
            self.snapshot.get_nested_region_ids(self.outer.id), [self.inner.id]
        )
        self.assertEqual(self.snapshot.get_nested_region_ids(self.inner.id), [])

    def test_get_nested_region_ids_includes_descendants_when_requested(self):
        self.assertCountEqual(
            self.snapshot.get_nested_region_ids(
                self.root.id, include_descendents=True
            ),
            [self.outer.id, self.sibling.id, self.inner.id],
        )
        self.assertEqual(
            self.snapshot.get_nested_region_ids(
                self.outer.id, include_descendents=True
            ),
            [self.inner.id],
        )
        self.assertEqual(
            self.snapshot.get_nested_region_ids(
                self.inner.id, include_descendents=True
            ),
            [],
        )
        self.assertEqual(
            self.snapshot.get_nested_region_ids(self.root.id),
            [self.outer.id, self.sibling.id],
        )
