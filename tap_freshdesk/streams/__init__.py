from tap_freshdesk.streams.tickets import Tickets
from tap_freshdesk.streams.conversations import Conversations
from tap_freshdesk.streams.contacts import Contacts
from tap_freshdesk.streams.companies import Companies
from tap_freshdesk.streams.satisfaction_ratings import SatisfactionRatings
from tap_freshdesk.streams.time_entries import TimeEntries
from tap_freshdesk.streams.agents import Agents
from tap_freshdesk.streams.groups import Groups
from tap_freshdesk.streams.roles import Roles

STREAMS = {
    "tickets": Tickets,
    "conversations": Conversations,
    "contacts": Contacts,
    "companies": Companies,
    "satisfaction_ratings": SatisfactionRatings,
    "time_entries": TimeEntries,
    "agents": Agents,
    "groups": Groups,
    "roles": Roles,
}