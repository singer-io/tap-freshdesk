# tap-freshdesk

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [freshdesk API].
- Extracts the following resources:
    - Tickets(https://github.com/singer-io)

    - Conversations(https://github.com/singer-io)

    - Contacts(https://github.com/singer-io)

    - Companies(https://github.com/singer-io)

    - Satisfaction_ratings(https://github.com/singer-io)

    - Time_entries(https://github.com/singer-io)

    - Agents(https://github.com/singer-io)

    - Groups(https://github.com/singer-io)

    - Roles(https://github.com/singer-io)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


** [tickets](https://github.com/singer-io)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [conversations](https://github.com/singer-io)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [contacts](https://github.com/singer-io)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [companies](https://github.com/singer-io)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [satisfaction_ratings](https://github.com/singer-io)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [time_entries](https://github.com/singer-io)**
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [agents](https://github.com/singer-io)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

** [groups](https://github.com/singer-io)**
- Primary keys: ['id']
- Replication strategy: FULL_TABLE

** [roles](https://github.com/singer-io)**
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
   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string, optional): Process and email for API logging purposes. Example: `tap-freshdesk <api_user_email@your_company.com>`
   - `request_timeout` (integer, `300`): Max time for which request should wait to get a response. Default request_timeout is 300 seconds.

    ```json
    {
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-freshdesk <api_user_email@your_company.com>",
        "request_timeout": 300,
        "api_key": "abcde12345"
        ...
    }
    ```
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "engage",
        "bookmarks": {
            "export": "2019-09-27T22:34:39.000000Z",
            "funnels": "2019-09-28T15:30:26.000000Z",
            "revenue": "2019-09-28T18:23:53Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-freshdesk --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md

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
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md
    ```bash
    > pylint tap_freshdesk -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools
    ```bash
    > tap-mixpanel --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
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
