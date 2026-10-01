from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class ScenarioAutomations(IncrementalStream):
    tap_stream_id = "scenario_automations"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "scenario_automations"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the scenario_automations stream."""
        return f"{self.client.base_url}/{self.path}"
