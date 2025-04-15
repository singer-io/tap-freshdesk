# Changelog

## 1.0.0
  * Complete refactoring of the tap [#60](https://github.com/singer-io/tap-freshdesk/pull/60)
  * Bump version of `requests` dependency to 2.32.3
  * singer-python upgrade to 6.1.0
  * Implements `agents`, `groups` and `roles` as FULL_TABLE, instead of, INCREMENTAL
  * API endpoint changes for `conversations`, `satisfaction_ratings` and `time_entries`

## 0.11.1
  * Dependabot update [#56](https://github.com/singer-io/tap-freshdesk/pull/56)

## 0.11.0
  * Add `integer` as a valid type for `groups.auto_ticket_assign` [#42](https://github.com/singer-io/tap-freshdesk/pull/42)

## 0.10.0
  * Adds `tickets_spam` and `tickets_deleted` streams [#26](https://github.com/singer-io/tap-freshdesk/pull/26)
  * Bump version of `requests` dependency to 2.20.0

## 0.9.0
  * Adds embedded fields to tickets endpoint [#19](https://github.com/singer-io/tap-freshdesk/pull/19)

## 0.8.2
  * Adds `try` logic around sync_tickets function to handle soft-deleted tickets [#18](https://github.com/singer-io/tap-freshdesk/pull/18)