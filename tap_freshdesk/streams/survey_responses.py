from typing import Dict

from singer import get_logger

from tap_freshdesk.streams.abstracts import (
    ChildBaseStream,
    TokenPaginatedMixin,
)

LOGGER = get_logger()


class SurveyResponses(TokenPaginatedMixin, ChildBaseStream):
    """Survey response records, fetched per parent CSAT survey.

    This is a child stream of csat_surveys.
    Ref: https://developers.freshdesk.com/api/#view_survey_responses
    """

    tap_stream_id = "survey_responses"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    # Path template: the survey id is substituted at runtime by
    # ChildBaseStream.get_url_endpoint via path.format(parent_obj["id"])
    path = "customer-satisfaction/surveys/{}/responses"
    parent = "csat_surveys"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Attach the parent survey id to every response record."""
        if parent_record:
            record["parent_id"] = parent_record["id"]
        record = super().modify_object(record, parent_record)
        return record
