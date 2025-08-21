from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, List
import copy

from singer import (
    metadata,
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
)
from singer.utils import strftime, strptime_to_utc

LOGGER = get_logger()


class BaseStream(ABC):
    """A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    page_size = 100
    headers = {"Accept": "application/json"}
    object_to_id = []
    children = []
    parent = ""
    bookmark_value = None

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []
        self.params = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> str:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    @property
    def selected_by_default(self) -> bool:
        """Indicates if a node in the schema should be replicated, if a user
        has not expressed any opinion on whether or not to replicate it."""
        return False

    def is_selected(self):
        return metadata.get(self.metadata, (), "selected")
    
    def is_child_selected(self, child):
        return child.metadata.get((), {}).get("selected", False)

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - transformer (object): A Object of the singer.transformer class.
         - parent_obj (dict): The parent object for the stream.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def get_records(self) -> List:
        """Interacts with api client interaction and pagination."""
        extraction_url = self.url_endpoint
        page_count = 1

        # Set initial params
        self.params.update({"per_page": self.page_size, "page": page_count})

        while True:
            LOGGER.info("Calling Page %s", page_count)
            response = self.client.get(
                extraction_url, self.params, self.headers, self.path
            )
            raw_records = response

            if not raw_records:
                LOGGER.warning("No records found on Page %s", page_count)
                break
            yield from raw_records

            if len(raw_records) == self.page_size:
                LOGGER.info("Fetching Page %s", page_count)
                page_count += 1
                self.params["page"] = page_count
            else:
                break

    def write_schema(self):
        """Write a schema message."""
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
            raise err

    def add_object_to_id(self, record: Dict) -> Dict:
        """Add object_to_id to the stream."""
        for key in self.object_to_id:
            if record[key] is not None:
                record[key + "_id"] = record[key]["id"]
            else:
                record[key + "_id"] = None

        return record

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = self.add_object_to_id(record)
        return record

    def modify_object_custom_fields(
        self, data, key_field="name", value_field="value", force_to_string=False
    ):
        # Custom fields are expected to be strings, but sometimes the API sends
        # booleans. We cast those to strings to match the schema.
        result = []
        for key, value in data.items():
            if force_to_string:
                value = str(value).lower()
            result.append({key_field: key, value_field: value})
        return result

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Get the URL endpoint for the stream"""
        return self.url_endpoint


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"
    config_start_key = "start_date"

    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config.get(self.config_start_key, False),
        )

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )
        value = max(current_bookmark, value)
        return write_bookmark(state, stream, key or self.replication_keys[0], value)

    def get_records(self, state: Dict) -> List:
        """Interacts with api client interaction and pagination."""
        extraction_url = self.url_endpoint
        page_count = 1

        # Set initial params
        if self.tap_stream_id in [
            "conversations",
            "satisfaction_ratings",
            "time_entries",
        ]:
            self.params = {"per_page": self.page_size, "page": page_count}
        elif self.tap_stream_id == "tickets":
            self.params.update({"per_page": self.page_size, "page": page_count})
        else:
            # Fetch the bookmark for incremental sync
            updated_since = self.get_bookmark(state, self.tap_stream_id)
            self.params.update(
                {
                    "per_page": self.page_size,
                    "updated_since": updated_since,
                    "page": page_count,
                }
            )

        while True:
            LOGGER.info("Calling Page %s", page_count)
            response = self.client.get(
                extraction_url, self.params, self.headers, self.path
            )
            raw_records = response

            if not raw_records:
                LOGGER.warning("No records found on Page %s", page_count)
                break
            yield from raw_records

            if len(raw_records) == self.page_size:
                LOGGER.info("Fetching Page %s", page_count)
                page_count += 1
                self.params["page"] = page_count
            else:
                break

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for `type: Incremental` stream."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(state):
                record = self.modify_object(record, parent_obj)
                if "custom_fields" in record:
                    record["custom_fields"] = self.modify_object_custom_fields(
                        record["custom_fields"], force_to_string=True
                    )
                transformed_record = transformer.transform(
                    copy.deepcopy(record), self.schema, self.metadata
                )

                record_timestamp = transformed_record[self.replication_keys[0]]
                if record_timestamp >= bookmark_date:
                    write_record(self.tap_stream_id, transformed_record)
                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )
                    counter.increment()

                    for child in self.child_to_sync:
                        child.sync(
                            state=state, transformer=transformer, parent_obj=record
                        )

            state = self.write_bookmark(state, self.tap_stream_id, value=current_max_bookmark_date)
            return counter.value


class FullTableStream(BaseStream):
    """Base Class for FULL_TABLE Stream."""

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    valid_replication_keys = None
    replication_keys = None

    total_records = 0

    def sync(self, state: Dict, transformer: Transformer) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()
            return counter.value

class ParentBaseStream(IncrementalStream):
    """Base Class for Parent Stream."""

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""

        min_parent_bookmark = (
            super().get_bookmark(state, stream) if self.is_selected() else None
        )
        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            child_bookmark = super().get_bookmark(
                state, child.tap_stream_id, key=bookmark_key
            )
            min_parent_bookmark = (
                min(min_parent_bookmark, child_bookmark)
                if min_parent_bookmark
                else child_bookmark
            )

        return min_parent_bookmark

    def write_bookmark(
        self, state: Dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """Write bookmark for parent and also propagate to children."""
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)

        for child in self.child_to_sync:
            bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
            # Write the parent's bookmark into the child state entry
            if hasattr(child, "write_child_bookmark_with_parent"):
                category_suffix = stream.replace(self.tap_stream_id, "")  # "", "_spam", "_deleted"
                child_state_bookmark = child.get_bookmark(state, child.tap_stream_id)
                child.write_child_bookmark_with_parent(
                    state,
                    category_suffix,
                    child_state_bookmark,
                    value
                )
            else:
                super().write_bookmark(
                    state, child.tap_stream_id, key=bookmark_key, value=value
                )

        return state

    def sync(
    self,
    state: Dict,
    transformer: Transformer,
    parent_obj: Dict = None,
) -> Dict:
        """Implementation for `type: Incremental` stream."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        # Set initial parameters for API call
        self.params.update(
            {
                "order_by": self.replication_keys[0],
                "order_type": "asc",
                "include": "requester,company,stats",
            }
        )

        with metrics.record_counter(self.tap_stream_id) as counter:
            filter_values = [{}, {"filter": "spam"}, {"filter": "deleted"}]
            for value in filter_values:
                if value:
                    ticket_key = self.tap_stream_id + "_" + value["filter"]
                else:
                    ticket_key = (
                        self.tap_stream_id
                    )  # Default key when value is None or empty
                current_max_bookmark_date = bookmark_date = updated_since = (
                    self.get_bookmark(state, ticket_key)
                )
                self.params.update({"updated_since": updated_since})
                self.params.update(**value)
                for record in self.get_records(state):
                    if "custom_fields" in record:
                        record["custom_fields"] = self.modify_object_custom_fields(
                            record["custom_fields"], force_to_string=True
                        )
                    transformed_record = transformer.transform(
                        record, self.schema, self.metadata
                    )

                    record_timestamp = transformed_record[self.replication_keys[0]]
                    if record_timestamp >= bookmark_date:
                        # Only write parent records if parent is selected
                        if self.is_selected():
                            write_record(self.tap_stream_id, transformed_record)
                            counter.increment()

                        # Sync only selected child streams
                        for child in self.child_to_sync:
                            if self.is_child_selected(child):
                                child.sync(
                                    state=state,
                                    transformer=transformer,
                                    parent_obj=record
                                )

                        current_max_bookmark_date = max(
                            current_max_bookmark_date, record_timestamp
                        )

                state = self.write_bookmark(
                    state, ticket_key, value=current_max_bookmark_date
                )
            return counter.value


class ChildBaseStream(IncrementalStream):
    """Base Class for Child Stream."""

    def get_url_endpoint(self, parent_obj=None):
        """Prepare URL endpoint for child streams."""
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """Singleton bookmark value for child streams."""
        if not self.bookmark_value:
            # Set bookmark value as singleton
            self.bookmark_value = super().get_bookmark(state, stream)

        return self.bookmark_value
    
    def get_parent_bookmark_for_category(self, state: Dict, category_key: str):
            """
            Reads the parent's bookmark date stored in the child's state for a given category.
            Falls back to parent's own bookmark if not present.
            """
            child_state = state.get("bookmarks", {}).get(f"{self.tap_stream_id}{category_key}", {})
            parent_key = f"{self.parent}{category_key}_updated_at"
            if parent_key in child_state:
                return child_state[parent_key]

            parent_state = state.get("bookmarks", {}).get(f"{self.parent}{category_key}", {})
            return parent_state.get("updated_at")

    def write_child_bookmark_with_parent(
        self,
        state: dict,
        category_key: str,
        child_bookmark_date: str,
        parent_bookmark_date: str
    ) -> dict:
        """
        Stores both child's own updated_at and parent's updated_at in the child bookmark.
        Example for conversations_spam:
        {
            "updated_at": "<child_date>",
            "tickets_spam_updated_at": "<parent_date>"
        }
        """
        child_stream_key = f"{self.tap_stream_id}{category_key}"
        parent_stream_key = f"{self.parent}{category_key}_updated_at"

        if "bookmarks" not in state:
            state["bookmarks"] = {}

        if child_stream_key not in state["bookmarks"]:
            state["bookmarks"][child_stream_key] = {}

        # Store child's bookmark
        if child_bookmark_date:
            state["bookmarks"][child_stream_key]["updated_at"] = child_bookmark_date

        # Store parent's bookmark alongside
        if parent_bookmark_date:
            state["bookmarks"][child_stream_key][parent_stream_key] = parent_bookmark_date

        return state

    def sync(self, state: Dict, transformer, parent_obj: Dict = None) -> Dict:
        if parent_obj is None:
            return 0  # No parent object means nothing to sync

        category_suffix = ""
        if "spam" in parent_obj.get("filter", ""):
            category_suffix = "_spam"
        elif "deleted" in parent_obj.get("filter", ""):
            category_suffix = "_deleted"

        parent_bookmark = self.get_parent_bookmark_for_category(state, category_suffix)

        # Get child's existing bookmark for this category
        child_bookmark = self.get_bookmark(state, f"{self.tap_stream_id}{category_suffix}")

        self.url_endpoint = self.get_url_endpoint(parent_obj)

        last_record_timestamp = child_bookmark  # Default to existing bookmark if no new records

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(state):
                transformed_record = transformer.transform(record, self.schema, self.metadata)
                record_timestamp = transformed_record[self.replication_keys[0]]

                # Compare against whichever is newer: child's own or parent's
                if record_timestamp >= (child_bookmark or parent_bookmark):
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    last_record_timestamp = record_timestamp

            # Update bookmark with both child's and parent's dates
            self.write_child_bookmark_with_parent(
                state,
                category_suffix,
                last_record_timestamp,
                parent_bookmark
            )

        return counter.value
