# Ice-Stream Data Quality Rules

## Purpose

The data quality layer validates checkout events before they are processed
by downstream components.

## Required Fields

Every checkout event must contain:

- `event_id`
- `timestamp`
- `user_id`
- `amount`
- `currency`

## Validation Rules

| Rule | Description |
|---|---|
| event_id_required | Event must contain an event ID |
| timestamp_required | Event must contain a timestamp |
| user_id_required | Event must contain a user ID |
| amount_required | Event must contain an amount |
| currency_required | Event must contain a currency |
| amount_must_be_numeric | Amount must be numeric |
| amount_must_be_non_negative | Amount cannot be negative |
| currency_must_be_string | Currency must be a string |
| currency_must_be_3_characters | Currency must contain exactly 3 characters |

## Valid Event Example

```json
{
  "event_id": "evt-001",
  "timestamp": "2026-08-29T10:00:00Z",
  "user_id": "user-123",
  "amount": 499.99,
  "currency": "INR"
}