import unittest
from unittest.mock import MagicMock, mock_open, patch

from tap_freshdesk import schema as schema_module


class ParentStream:
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    replication_method = "INCREMENTAL"
    parent = ""


class ChildStream:
    key_properties = ["id"]
    replication_keys = None
    replication_method = "FULL_TABLE"
    parent = "parent"


class TestSchemaHelpers(unittest.TestCase):
    def test_get_abs_path_returns_joined_path(self):
        result = schema_module.get_abs_path("schemas/a.json")
        self.assertTrue(result.endswith("tap_freshdesk/schemas/a.json"))

    @patch("tap_freshdesk.schema.os.path.isfile", return_value=True)
    @patch("tap_freshdesk.schema.os.listdir", return_value=["a.json", "b.json"])
    @patch("tap_freshdesk.schema.os.path.exists", return_value=True)
    @patch("tap_freshdesk.schema.get_abs_path", return_value="/tmp/shared")
    @patch("builtins.open", new_callable=mock_open)
    @patch("tap_freshdesk.schema.json.load")
    def test_load_schema_references_reads_shared_files(
        self,
        mock_json_load,
        _mock_open,
        _mock_get_abs_path,
        _mock_exists,
        _mock_listdir,
        _mock_isfile,
    ):
        mock_json_load.side_effect = [{"a": 1}, {"b": 2}]

        refs = schema_module.load_schema_references()

        self.assertEqual(refs["shared/a.json"], {"a": 1})
        self.assertEqual(refs["shared/b.json"], {"b": 2})

    @patch("tap_freshdesk.schema.os.path.exists", return_value=False)
    @patch("tap_freshdesk.schema.get_abs_path", return_value="/tmp/missing")
    def test_load_schema_references_missing_shared_dir(
        self,
        _mock_get_abs_path,
        _mock_exists,
    ):
        refs = schema_module.load_schema_references()
        self.assertEqual(refs, {})


class TestGetSchemas(unittest.TestCase):
    @patch("tap_freshdesk.schema.metadata.to_list")
    @patch("tap_freshdesk.schema.metadata.write")
    @patch("tap_freshdesk.schema.metadata.to_map")
    @patch("tap_freshdesk.schema.metadata.get_standard_metadata")
    @patch("tap_freshdesk.schema.metadata.new")
    @patch("tap_freshdesk.schema.singer.resolve_schema_references")
    @patch("tap_freshdesk.schema.load_schema_references", return_value={})
    @patch("tap_freshdesk.schema.get_abs_path")
    @patch("builtins.open", new_callable=mock_open)
    @patch("tap_freshdesk.schema.json.load")
    def test_get_schemas_sets_parent_metadata_and_automatic_keys(
        self,
        mock_json_load,
        _mock_open,
        mock_get_abs_path,
        _mock_load_refs,
        mock_resolve_refs,
        mock_metadata_new,
        mock_std_metadata,
        mock_to_map,
        mock_metadata_write,
        mock_to_list,
    ):
        parent_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "updated_at": {"type": "string"},
            },
        }
        child_schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        }

        mock_json_load.side_effect = [parent_schema, child_schema]
        mock_get_abs_path.side_effect = [
            "/tmp/schemas_parent.json",
            "/tmp/schemas_child.json",
        ]
        mock_resolve_refs.side_effect = lambda schema, refs: schema
        mock_metadata_new.return_value = "new-metadata"
        mock_std_metadata.return_value = "std-metadata"
        mock_to_map.return_value = {}
        mock_metadata_write.side_effect = lambda mdata, *_args: mdata
        mock_to_list.return_value = [{"metadata": {}, "breadcrumb": []}]

        with patch.dict(
            "tap_freshdesk.schema.STREAMS",
            {"parent": ParentStream, "child": ChildStream},
            clear=True,
        ):
            schemas, field_metadata = schema_module.get_schemas()

        self.assertEqual(set(schemas.keys()), {"parent", "child"})
        self.assertEqual(set(field_metadata.keys()), {"parent", "child"})

        # Parent linkage should be written for child stream metadata.
        self.assertIn(
            unittest.mock.call({}, (), "parent-tap-stream-id", "parent"),
            mock_metadata_write.mock_calls,
        )

        # Automatic inclusion should be set for parent.updated_at.
        self.assertIn(
            unittest.mock.call(
                {},
                ("properties", "updated_at"),
                "inclusion",
                "automatic",
            ),
            mock_metadata_write.mock_calls,
        )


class TestWriteSchema(unittest.TestCase):
    def test_write_schema_recurses_and_appends_selected_child(self):
        parent_stream = MagicMock()
        parent_stream.is_selected.return_value = True
        parent_stream.children = ["child"]
        parent_stream.child_to_sync = []

        child_stream = MagicMock()
        child_stream.is_selected.return_value = True
        child_stream.children = []

        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        with patch.dict(
            "tap_freshdesk.schema.STREAMS",
            {"child": lambda _client, _catalog_stream: child_stream},
            clear=True,
        ):
            schema_module.write_schema(
                parent_stream,
                client=MagicMock(),
                streams_to_sync=["child"],
                catalog=catalog,
            )

        parent_stream.write_schema.assert_called_once()
        child_stream.write_schema.assert_called_once()
        self.assertEqual(parent_stream.child_to_sync, [child_stream])

    def test_write_schema_does_not_append_unselected_child(self):
        parent_stream = MagicMock()
        parent_stream.is_selected.return_value = True
        parent_stream.children = ["child"]
        parent_stream.child_to_sync = []

        child_stream = MagicMock()
        child_stream.is_selected.return_value = True
        child_stream.children = []

        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        with patch.dict(
            "tap_freshdesk.schema.STREAMS",
            {"child": lambda _client, _catalog_stream: child_stream},
            clear=True,
        ):
            schema_module.write_schema(
                parent_stream,
                client=MagicMock(),
                streams_to_sync=[],
                catalog=catalog,
            )

        parent_stream.write_schema.assert_called_once()
        child_stream.write_schema.assert_called_once()
        self.assertEqual(parent_stream.child_to_sync, [])
