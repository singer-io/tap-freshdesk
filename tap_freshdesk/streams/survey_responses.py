from typing import Dict

from singer import get_logger

from tap_freshdesk.streams.abstracts import (
    ChildBaseStream,
    TokenPaginatedMixin,
)
from tap_freshdesk.exceptions import freshdeskNotFoundError

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
    data_key = "data"

    def check_access(self) -> bool:
        """Verify credentials can access the survey_responses endpoint.

        Delegates to the base class which fetches a real parent survey record
        and uses its id to probe the responses endpoint.

        The only difference from the base behaviour: a 404 on the responses
        endpoint is **not** a permission error — it just means the probed
        survey has no responses yet.  In that case we return ``True`` and let
        ``sync()`` handle per-survey 404s gracefully.

        401 / 403 responses still mean genuine permission denial and cause
        the stream to be excluded from the catalog.
        """
        try:
            return super().check_access()
        except freshdeskNotFoundError:
            LOGGER.info(
                "Stream '%s': probe returned 404 (survey has no responses)."
                " Treating as accessible; sync will skip empty surveys.",
                self.tap_stream_id,
            )
            return True

    def sync(self, state: Dict, transformer, parent_obj: Dict = None) -> Dict:
        """Sync responses for a single parent survey.

        Overrides ChildBaseStream.sync to catch 404 errors raised when a
        survey has no responses, log a warning, and continue to the next
        survey instead of aborting the run.
        """
        if parent_obj is None:
            return 0

        self.url_endpoint = self.get_url_endpoint(parent_obj)
        try:
            return super().sync(
                state=state,
                transformer=transformer,
                parent_obj=parent_obj,
            )
        except freshdeskNotFoundError:
            LOGGER.warning(
                "Survey '%s' has no responses (404). "
                "Skipping and continuing with the next survey.",
                parent_obj.get("id"),
            )
            return 0

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Attach the parent survey id to every response record."""
        if parent_record:
            record["parent_id"] = parent_record["id"]
        record = super().modify_object(record, parent_record)
        return record
