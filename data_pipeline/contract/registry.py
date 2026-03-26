# =============================================================================
# Contract Stage Module Registry
# =============================================================================

from data_pipeline.contract.contract_logic import (
    deduplicate_exact_events,
    remove_impossible_timestamps,
    remove_unparsable_timestamps,
    remove_rows_with_null_constraint,
    cascade_drop_by_order_id,
    enforce_parent_reference,
    enforce_schema,
)

ROLE_STEPS = {
    "event_fact": [
        {
            "contract": deduplicate_exact_events,
            "metric": "deduplicated_rows",
            "args": [],
            "return_invalid_ids": False,
        },
        {
            "contract": remove_unparsable_timestamps,
            "metric": "removed_unparsable_timestamps",
            "args": [],
            "return_invalid_ids": True,
        },
        {
            "contract": remove_impossible_timestamps,
            "metric": "removed_impossible_timestamps",
            "args": [],
            "return_invalid_ids": True,
        },
        {
            "contract": remove_rows_with_null_constraint,
            "metric": "removed_null_values",
            "args": ["non_nullable"],
            "return_invalid_ids": True,
        },
        {
            "contract": enforce_schema,
            "metric": "removed_column",
            "args": ["required_column"],
            "return_invalid_ids": False,
        },
    ],
    "transaction_detail": [
        {
            "contract": deduplicate_exact_events,
            "metric": "deduplicated_rows",
            "args": [],
            "return_invalid_ids": False,
        },
        {
            "contract": remove_rows_with_null_constraint,
            "metric": "removed_null_values",
            "args": ["non_nullable"],
            "return_invalid_ids": True,
        },
        {
            "contract": cascade_drop_by_order_id,
            "metric": "removed_cascade_rows",
            "args": ["invalid_order_ids"],
            "return_invalid_ids": False,
        },
        {
            "contract": enforce_parent_reference,
            "metric": "removed_ghost_orphan_rows",
            "args": ["valid_order_ids"],
            "return_invalid_ids": False,
        },
        {
            "contract": enforce_schema,
            "metric": "removed_column",
            "args": ["required_column"],
            "return_invalid_ids": False,
        },
    ],
    "entity_reference": [
        {
            "contract": deduplicate_exact_events,
            "metric": "deduplicated_rows",
            "args": [],
            "return_invalid_ids": False,
        },
        {
            "contract": remove_rows_with_null_constraint,
            "metric": "removed_null_values",
            "args": ["non_nullable"],
            "return_invalid_ids": True,
        },
        {
            "contract": enforce_schema,
            "metric": "removed_column",
            "args": ["required_column"],
            "return_invalid_ids": False,
        },
    ],
}
