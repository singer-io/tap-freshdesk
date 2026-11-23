from typing import Any, Dict, Iterator

from singer import get_logger

from tap_freshdesk.streams.abstracts import ChildBaseStream

LOGGER = get_logger()


def normalize_ratings(ratings: Any) -> Any:
    """Convert the API's ratings object into the schema's array format.

    The Freshdesk API returns ratings as an object keyed by question, e.g.
    {"default_question": 103, "question_2": 102}, while the schema expects
    [{"question": "default_question", "value": 103}, ...].
    Lists and None are returned unchanged.
    """
    if not isinstance(ratings, dict):
        return ratings
    normalized = []
    for question, value in ratings.items():
        try:
            value = int(value) if value is not None else None
        except (TypeError, ValueError):
            value = None
        normalized.append({"question": question, "value": value})
    return normalized


class SatisfactionRatings(ChildBaseStream):
    tap_stream_id = "satisfaction_ratings"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "tickets/{}/satisfaction_ratings"
    parent = "tickets"

    def get_records(self, state: Dict) -> Iterator[Dict]:
        """Yield records with `ratings` converted to the schema's array format."""
        for record in super().get_records(state):
            if "ratings" in record:
                record["ratings"] = normalize_ratings(record["ratings"])
            yield record

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record["parent_id"] = parent_record["id"]
        record = super().modify_object(record, parent_record)
        return record
