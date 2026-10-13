import unittest
import importlib
from unittest.mock import MagicMock, patch

sync_module = importlib.import_module("tap_freshdesk.sync")


class _Selected:
    def __init__(self, stream):
        self.stream = stream


class ParentStreamObj:
    parent = ""

    def __init__(self, client, _catalog_stream):
        self.client = client
        self.sync = MagicMock(return_value=7)


class ChildStreamObj:
    parent = "parent"

    def __init__(self, client, _catalog_stream):
        self.client = client
        self.sync = MagicMock(return_value=0)


class SyncTests(unittest.TestCase):
    @patch("tap_freshdesk.sync.singer.write_state")
    @patch("tap_freshdesk.sync.singer.set_currently_syncing")
    @patch(
        "tap_freshdesk.sync.singer.get_currently_syncing",
        return_value="tickets",
    )
    def test_update_currently_syncing_clears_key(
        self,
        _mock_get,
        mock_set,
        mock_write,
    ):
        state = {"currently_syncing": "tickets"}

        sync_module.update_currently_syncing(state, None)

        self.assertNotIn("currently_syncing", state)
        mock_set.assert_not_called()
        mock_write.assert_called_once_with(state)

    @patch("tap_freshdesk.sync.singer.write_state")
    @patch("tap_freshdesk.sync.singer.set_currently_syncing")
    @patch(
        "tap_freshdesk.sync.singer.get_currently_syncing",
        return_value=None,
    )
    def test_update_currently_syncing_sets_value(
        self,
        _mock_get,
        mock_set,
        mock_write,
    ):
        state = {}

        sync_module.update_currently_syncing(state, "tickets")

        mock_set.assert_called_once_with(state, "tickets")
        mock_write.assert_called_once_with(state)

    @patch("tap_freshdesk.sync.write_schema")
    @patch("tap_freshdesk.sync.update_currently_syncing")
    @patch(
        "tap_freshdesk.sync.singer.get_currently_syncing",
        return_value=None,
    )
    @patch("tap_freshdesk.sync.singer.Transformer")
    def test_sync_adds_parent_when_only_child_selected(
        self,
        mock_transformer,
        _mock_currently_syncing,
        mock_update_currently_syncing,
        mock_write_schema,
    ):
        transformer_obj = MagicMock()
        mock_transformer.return_value.__enter__.return_value = transformer_obj

        catalog = MagicMock()
        catalog.get_selected_streams.return_value = [_Selected("child")]
        catalog.get_stream.side_effect = (
            lambda name: MagicMock(name=f"catalog_{name}")
        )

        parent_instance = ParentStreamObj(MagicMock(), MagicMock())
        child_instance = ChildStreamObj(MagicMock(), MagicMock())

        def stream_factory(name):
            if name == "parent":
                return lambda client, catalog_stream: parent_instance
            return lambda client, catalog_stream: child_instance

        with patch.dict(
            "tap_freshdesk.sync.STREAMS",
            {
                "child": stream_factory("child"),
                "parent": stream_factory("parent"),
            },
            clear=True,
        ):
            sync_module.sync(
                client=MagicMock(),
                config={},
                catalog=catalog,
                state={},
            )

        # Child stream is skipped in main loop, parent stream gets synced.
        child_instance.sync.assert_not_called()
        parent_instance.sync.assert_called_once_with(
            state={},
            transformer=transformer_obj,
        )
        mock_write_schema.assert_called_once()
        # Start + finish calls for parent.
        self.assertEqual(mock_update_currently_syncing.call_count, 2)

    @patch("tap_freshdesk.sync.singer.metadata.to_map", return_value={})
    def test_collect_child_to_sync_builds_child_and_recurses(
        self,
        _mock_to_map,
    ):
        parent = MagicMock()
        parent.children = ["child"]
        parent.child_to_sync = []

        child = MagicMock()
        child.children = []

        catalog = MagicMock()
        child_catalog = MagicMock()
        child_catalog.schema.to_dict.return_value = {
            "type": "object",
            "properties": {},
        }
        child_catalog.metadata = []
        catalog.get_stream.return_value = child_catalog

        with patch.dict(
            "tap_freshdesk.sync.STREAMS",
            {"child": lambda _client, _schema, _metadata: child},
            clear=True,
        ):
            sync_module.collect_child_to_sync(
                stream=parent,
                client=MagicMock(),
                selected_streams=["child"],
                catalog=catalog,
            )

        self.assertEqual(parent.child_to_sync, [child])
        child.write_schema.assert_called_once()
