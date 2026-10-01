from tap_freshdesk.streams.tickets import Tickets
from tap_freshdesk.streams.conversations import Conversations
from tap_freshdesk.streams.contacts import Contacts
from tap_freshdesk.streams.companies import Companies
from tap_freshdesk.streams.satisfaction_ratings import SatisfactionRatings
from tap_freshdesk.streams.time_entries import TimeEntries
from tap_freshdesk.streams.agents import Agents
from tap_freshdesk.streams.groups import Groups
from tap_freshdesk.streams.roles import Roles
from tap_freshdesk.streams.account import Account
from tap_freshdesk.streams.ticket_fields import TicketFields
from tap_freshdesk.streams.ticket_forms import TicketForms
from tap_freshdesk.streams.contact_fields import ContactFields
from tap_freshdesk.streams.skills import Skills
from tap_freshdesk.streams.company_fields import CompanyFields
from tap_freshdesk.streams.email_configs import EmailConfigs
from tap_freshdesk.streams.email_mailboxes import EmailMailboxes
from tap_freshdesk.streams.products import Products
from tap_freshdesk.streams.business_hours import BusinessHours
from tap_freshdesk.streams.scenario_automations import ScenarioAutomations
from tap_freshdesk.streams.sla_policies import SlaPolicies
from tap_freshdesk.streams.surveys import Surveys
from tap_freshdesk.streams.csat_surveys import CsatSurveys
from tap_freshdesk.streams.survey_responses import SurveyResponses

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
    "account": Account,
    "ticket_fields": TicketFields,
    "ticket_forms": TicketForms,
    "contact_fields": ContactFields,
    "skills": Skills,
    "company_fields": CompanyFields,
    "email_configs": EmailConfigs,
    "email_mailboxes": EmailMailboxes,
    "products": Products,
    "business_hours": BusinessHours,
    "scenario_automations": ScenarioAutomations,
    "sla_policies": SlaPolicies,
    "surveys": Surveys,
    "csat_surveys": CsatSurveys,
    "survey_responses": SurveyResponses,
}
