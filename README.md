# tap-freshdesk

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from the [freshdesk API](https://developers.freshdesk.com/api/).
- Extracts the following resources from Freshdesk::
    - [Tickets](https://developers.freshdesk.com/api/#list_all_tickets)

    - [Conversations](https://developers.freshdesk.com/api/#list_all_ticket_notes)

    - [Contacts](https://developers.freshdesk.com/api/#list_all_contacts)

    - [Companies](https://developers.freshdesk.com/api/#list_all_companies)

    - [Satisfaction Ratings](https://developers.freshdesk.com/api/#view_ticket_satisfaction_ratings)

    - [Time Entries](https://developers.freshdesk.com/api/#list_all_ticket_timeentries)

    - [Agents](https://developers.freshdesk.com/api/#list_all_agents)

    - [Groups](https://developers.freshdesk.com/api/#list_all_groups)

    - [Roles](https://developers.freshdesk.com/api/#list_all_roles)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


**[tickets](https://developers.freshdesk.com/api/#list_all_tickets)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[conversations](https://developers.freshdesk.com/api/#list_all_ticket_notes)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[contacts](https://developers.freshdesk.com/api/#list_all_contacts)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[companies](https://developers.freshdesk.com/api/#list_all_companies)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[satisfaction_ratings](https://developers.freshdesk.com/api/#view_ticket_satisfaction_ratings)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[time_entries](https://developers.freshdesk.com/api/#list_all_ticket_timeentries)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

**[agents](https://developers.freshdesk.com/api/#list_all_agents)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[groups](https://developers.freshdesk.com/api/#list_all_groups)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

**[roles](https://developers.freshdesk.com/api/#list_all_roles)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE



## Authentication

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-freshdesk
    > pip install -e .
    ```
2. Get your Freshdesk API Key

    Login to your Freshdesk account, navigate to your profile settings
    page, and save "Your API Token", you'll need it for the next step.


    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
    Create a JSON file called `config.json` containing the api token you just found and
    the subdomain to your Freshdesk account. The subdomain will take the format
    `subdomain.freshdesk.com`.

    ```json
    {
      "api_key": "your-api-token",
      "domain": "subdomain",
      "start_date": "2017-01-17T20:32:05Z"
    }
    ```
    ```
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "agents",
        "bookmarks": {
            "companies": "2019-09-27T22:34:39.000000Z",
            "contacts": "2019-09-28T15:30:26.000000Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-freshdesk --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md)

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md)

    For Sync mode:
    ```bash
    > tap-freshdesk --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-freshdesk --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-freshdesk --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap

    While developing the freshdesk tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md)
    ```bash
    > pylint tap_freshdesk -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools)
    ```bash
    > tap-freshdesk --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .'[dev]'
    ```
---

Copyright &copy; 2017 Stitch
