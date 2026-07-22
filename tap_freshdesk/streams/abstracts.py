from abc import ABC, abstractmethod
from datetime import timedelta
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
from urllib.parse import parse_qs, urlparse

from singer.utils import strftime, strptime_to_utc

from tap_freshdesk.exceptions import (
    freshdeskForbiddenError,
    freshdeskNotFoundError,
    freshdeskUnauthorizedError,
)

from tap_freshdesk.exceptions import freshdeskBadRequestError

LOGGER = get_logger()


class TokenPaginatedMixin:
    """Mixin for streams whose API wraps records under a ``data`` key and
    paginates using ``paging.next`` / ``paging.previous`` full URLs carrying
    a ``next_token`` query parameter (instead of a ``page`` number).

    Must appear **before** the base stream class in the class definition so
    that Python's MRO resolves ``get_records`` from this mixin first.

    Expected response shape::

        {
            "data": [...],
            "paging": {
                "next": "https://…?per_page=100&next_token=<token>",
                "previous": null
            }
        }
    """

    data_key = "data"  # default; subclasses may override

    def get_records(self, state=None):  # noqa: D102
        """Yield records using token-based (``next_token``) pagination."""
        extraction_url = self.url_endpoint
        self.params = {"per_page": self.page_size}
        page_count = 1

        while True:
            LOGGER.info("Calling Page %s", page_count)
            response = self.client.get(
                extraction_url, self.params, self.headers, self.path
            )

            raw_records = (
                response.get(self.data_key, []) if isinstance(response, dict) else []
            )

            if not raw_records:
                LOGGER.warning("No records found on Page %s", page_count)
                break

            yield from raw_records

            next_url = (
                (response.get("paging") or {}).get("next")
                if isinstance(response, dict)
                else None
            )
            if not next_url:
                break

            next_token = parse_qs(urlparse(next_url).query).get(
                "next_token", [None]
            )[0]
            if not next_token:
                break

            self.params = {
                "per_page": self.page_size,
                "next_token": next_token,
            }
            page_count += 1


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
        self.schema = catalog.schema.to_dict() if catalog else {}
        self.metadata = metadata.to_map(catalog.metadata) if catalog else {}
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
        return metadata.get(child.metadata, (), "selected")

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

    def check_access(self) -> bool:
        """Verify that the API credentials have read access to this stream."""
        endpoint = f"{self.client.base_url}/{self.path}"
        params = {"per_page": 1, "page": 1}
        try:
            if self.parent:
                from tap_freshdesk.streams import STREAMS  # To fix circular import
                parent_params = {"per_page": 1, "page": 1}

                if self.parent == "tickets":
                    updated_since = self.get_bookmark({}, self.tap_stream_id)
                    parent_params["updated_since"] = updated_since

                parent_stream = STREAMS[self.parent](client=self.client)
                parent_endpoint = f"{self.client.base_url}/{parent_stream.path}"
                parent_records = self.client.get(
                    endpoint=parent_endpoint,
                    params=parent_params,
                    headers=self.headers,
                )

                if not parent_records:
                    LOGGER.warning(
                        "Stream '%s' could not be probed because parent stream '%s' has no records.",
                        self.tap_stream_id,
                        self.parent,
                    )
                    return True

                if hasattr(self, "data_key"):
                    parent_records = parent_records.get(self.data_key, []) if isinstance(parent_records, dict) else parent_records

                endpoint = f"{self.client.base_url}/{self.path.format(parent_records[0]['id'])}"

            self.client.get(
                endpoint=endpoint,
                params=params,
                headers=self.headers,
            )
            return True
        except (freshdeskUnauthorizedError, freshdeskForbiddenError) as err:
            LOGGER.warning(
                "Permission Error: Stream '%s' - %s",
                self.tap_stream_id,
                err,
            )
            return False
        except freshdeskNotFoundError as err:
            if "Account not found for the provided domain" in str(err):
                LOGGER.warning(
                    "Permission Error: Stream '%s' - %s",
                    self.tap_stream_id,
                    err,
                )
                return False
            raise


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
            "ticket_fields",
            "email_configs",
            "email_mailboxes",
            "business_hours",
            "scenario_automations",
            "sla_policies",
            "ticket_forms",
            "products",
            "skills",
            "surveys"
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
                page_count += 1
                self.params["page"] = page_count
                LOGGER.info("Fetching Page %s", page_count)
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
        self.url_endpoint = self.get_url_endpoint()
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

        # Maximum number of retries when the Freshdesk Tickets API limit
        # (300 pages / 30,000 tickets) is reached.
        # This prevents the sync from entering an infinite retry loop if the limit is encountered repeatedly.
        MAX_RESTARTS = 10

        # Tracks IDs processed for the current bookmark window to avoid duplicates.
        ids_cache = set()
        current_timestamp_window = None

        with metrics.record_counter(self.tap_stream_id) as counter:
            filter_values = [{}, {"filter": "spam"}, {"filter": "deleted"}]
            for value in filter_values:
                if value:
                    ticket_key = self.tap_stream_id + "_" + value["filter"]
                else:
                    ticket_key = (
                        self.tap_stream_id
                    )  # Default key when value is None or empty

                # Added a try/except workflow to gracefully handle the Freshdesk Tickets API limit.
                # The Tickets endpoint returns a maximum of 300 pages (30,000 tickets) in a single request.
                # Requests exceeding this limit result in a 400 response, so we catch the error
                # and handle it to allow the sync to continue from the appropriate bookmark.
                # Reference: https://developers.freshdesk.com/api/#list_all_tickets
                restart_count = 0  # Counter check for max 400 error retries
                sync_completed = False  # Flag to handle the while loop

                while not sync_completed:  # If the sync was interrupted, the core workflow will be resumed with updated state

                    current_max_bookmark_date = bookmark_date = updated_since = (
                        self.get_bookmark(state, ticket_key)
                    )
                    self.params.update({"updated_since": updated_since})
                    self.params.update(**value)

                    try:
                        for record in self.get_records(state):

                            if "custom_fields" in record:
                                record["custom_fields"] = self.modify_object_custom_fields(
                                    record["custom_fields"], force_to_string=True
                                )
                            transformed_record = transformer.transform(
                                record, self.schema, self.metadata
                            )

                            record_timestamp = transformed_record[self.replication_keys[0]]

                            # Handle records grouped by timestamp.
                            # Reset the cached IDs whenever the timestamp changes to ensure
                            # each record is processed only once within a given timestamp.
                            # Example:
                            # 10:00 -> cache {1,2,3}
                            # 10:01 -> clear cache
                            # 10:02 -> clear cache
                            if current_timestamp_window != record_timestamp:
                                LOGGER.info(
                                    "Moving timestamp window from %s to %s. "
                                    "Clearing ID cache.",
                                    current_timestamp_window,
                                    record_timestamp,
                                )

                                current_timestamp_window = record_timestamp
                                ids_cache.clear()

                            # Check if the record has already been synced using the ids_cache to avoid duplicates
                            cache_key = ticket_key + "_" + str(record["id"])
                            if cache_key in ids_cache:
                                LOGGER.info(
                                    "Skipping already processed ticket id=%s "
                                    "for timestamp=%s",
                                    cache_key,
                                    record_timestamp,
                                )
                                continue

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

                                # Add to cache ONLY after successful processing
                                ids_cache.add(cache_key)

                                current_max_bookmark_date = max(
                                    current_max_bookmark_date, record_timestamp
                                )

                        # Sync completed successfully.
                        state = self.write_bookmark(
                            state,
                            ticket_key,
                            value=current_max_bookmark_date,
                        )

                        sync_completed = True

                    except freshdeskBadRequestError:
                        restart_count += 1
                        # Handle a Freshdesk bad request error by restarting the sync with the
                        # bookmark reset to current_max_bookmark_date.
                        # Ref: https://developers.freshdesk.com/api/#list_all_tickets
                        #
                        # If the number of restart attempts exceeds MAX_RESTARTS, advance the
                        # bookmark and terminate the current sync. The next sync will resume from
                        # the updated bookmark.
                        #
                        # Corner case:
                        # If the bookmark points to a timestamp with more than 30,000 tickets,
                        # every retry will fail because the API request parameters remain
                        # unchanged. To prevent an endless retry loop, advance the bookmark by
                        # one second and abort the current sync.

                        LOGGER.warning(
                            "Freshdesk ticket limit reached for %s. "
                            "Restarting sync from bookmark %s "
                            "(attempt %s/%s)",
                            ticket_key,
                            current_max_bookmark_date,
                            restart_count,
                            MAX_RESTARTS,
                        )

                        # If the max restart attempts are reached, advance the bookmark and abort the sync.
                        if restart_count >= MAX_RESTARTS:
                            advanced_bookmark = strftime(
                                strptime_to_utc(current_max_bookmark_date)
                                + timedelta(seconds=1)
                            )

                            LOGGER.error(
                                "Max restart attempts reached for %s. "
                                "Advancing bookmark from %s to %s and aborting sync.",
                                ticket_key,
                                current_max_bookmark_date,
                                advanced_bookmark,
                            )

                            state = self.write_bookmark(
                                state,
                                ticket_key,
                                value=advanced_bookmark,
                            )

                            raise

                        # Persist progress before restarting
                        state = self.write_bookmark(
                            state,
                            ticket_key,
                            value=current_max_bookmark_date,
                        )

                        # Loop restarts automatically

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
                if record_timestamp >= max(filter(None, [child_bookmark, parent_bookmark])):
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
