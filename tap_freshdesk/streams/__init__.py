from tap_freshdesk.streams.tickets import Tickets
from tap_freshdesk.streams.conversations import Conversations
from tap_freshdesk.streams.contacts import Contacts
from tap_freshdesk.streams.companies import Companies
from tap_freshdesk.streams.satisfaction_ratings import Satisfaction_ratings
from tap_freshdesk.streams.time_entries import Time_entries
from tap_freshdesk.streams.agents import Agents
from tap_freshdesk.streams.groups import Groups
from tap_freshdesk.streams.roles import Roles

STREAMS = {
    'tickets': Tickets,
    'conversations': Conversations,
    'contacts': Contacts,
    'companies': Companies,
    'satisfaction_ratings': Satisfaction_ratings,
    'time_entries': Time_entries,
    'agents': Agents,
    'groups': Groups,
    'roles': Roles,
}