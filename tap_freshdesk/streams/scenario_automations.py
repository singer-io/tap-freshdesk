from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class ScenarioAutomations(IncrementalStream):
    tap_stream_id = "scenario_automations"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "scenario_automations"
