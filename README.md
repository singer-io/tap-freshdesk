# tap-freshdesk

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from Freshdesk's [REST API](https://developer.freshdesk.com/api/)
- Extracts the following resources from Freshdesk:
  - [Agents](https://developer.freshdesk.com/api/#agents)
  - [Companies](https://developer.freshdesk.com/api/#companies)
  - [Contacts](https://developer.freshdesk.com/api/#contacts)
  - [Conversations](https://developer.freshdesk.com/api/#conversations)
  - [Groups](https://developer.freshdesk.com/api/#groups)
  - [Roles](https://developer.freshdesk.com/api/#roles)
  - [Satisfaction Ratings](https://developer.freshdesk.com/api/#satisfaction-ratings)
  - [Tickets](https://developer.freshdesk.com/api/#tickets)
  - [Time Entries](https://developer.freshdesk.com/api/#time-entries)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Quick start

1. Install

    ```bash
    > pip install tap-freshdesk
    ```

2. Get your Freshdesk API Key

    Login to your Freshdesk account, navigate to your profile settings
    page, and save "Your API Token", you'll need it for the next step.

3. Create the config file

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

4. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the API endpoints
    to force the application to only fetch data newer than those dates.
    If you omit the file it will fetch all Freshdesk data

    ```json
    {"tickets": "2017-01-17T20:32:05Z",
    "agents": "2017-01-17T20:32:05Z",
    "roles": "2017-01-17T20:32:05Z",
    "groups": "2017-01-17T20:32:05Z",
    "companies": "2017-01-17T20:32:05Z",
    "contacts": "2017-01-17T20:32:05Z"}
    ```

5. Run the application

    `tap-freshdesk` can be run with:

    ```bash
    tap-freshdesk --config config.json [--state state.json]
    ```

---

Copyright &copy; 2017 Stitch
