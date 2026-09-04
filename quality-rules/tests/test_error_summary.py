from models import ErrorSummary, create_error_summary


def test_error_summary():
    errors = [
        {
            "field": "quantity",
            "code": "NON_POSITIVE_VALUE",
            "message": "Quantity must be greater than 0",
        },
        {
            "field": "amount",
            "code": "NEGATIVE_VALUE",
            "message": "Amount cannot be negative",
        },
    ]

    summary = create_error_summary("evt_101", errors)

    assert isinstance(summary, ErrorSummary)
    assert summary.event_id == "evt_101"
    assert summary.number_of_errors == 2
    assert summary.error_codes == [
        "NON_POSITIVE_VALUE",
        "NEGATIVE_VALUE",
    ]


def test_empty_error_summary():
    summary = create_error_summary("evt_102", [])

    assert summary.event_id == "evt_102"
    assert summary.number_of_errors == 0
    assert summary.error_codes == []


def test_error_summary_ignores_missing_code():
    errors = [
        {
            "field": "quantity",
            "message": "Quantity is invalid",
        },
        {
            "field": "amount",
            "code": "NEGATIVE_VALUE",
            "message": "Amount cannot be negative",
        },
    ]

    summary = create_error_summary("evt_103", errors)

    assert summary.number_of_errors == 2
    assert summary.error_codes == ["NEGATIVE_VALUE"]