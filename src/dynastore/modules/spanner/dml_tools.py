# dynastore/modules/spanner/dml_tools.py
import logging
from typing import Optional, Any
from datetime import datetime, date
from google.cloud import spanner_v1
from google.cloud.spanner_v1 import transaction

logger = logging.getLogger(__name__)

# --- Type Inference Logic ---
PYTHON_TYPE_TO_SPANNER_TYPE_CODE = {
    int: spanner_v1.types.TypeCode.INT64,
    str: spanner_v1.types.TypeCode.STRING,
    bool: spanner_v1.types.TypeCode.BOOL,
    float: spanner_v1.types.TypeCode.FLOAT64,
    bytes: spanner_v1.types.TypeCode.BYTES,
    datetime: spanner_v1.types.TypeCode.TIMESTAMP,
    date: spanner_v1.types.TypeCode.DATE,
}

def infer_spanner_type(value):
    """Infers Spanner type code from Python type for basic types and simple arrays."""
    if isinstance(value, list):
        if not value:
            raise ValueError(
                "Cannot infer Spanner type for an empty list without element type information."
            )
        element_type_code = PYTHON_TYPE_TO_SPANNER_TYPE_CODE.get(
            type(value[0]))
        if not element_type_code:
            raise ValueError(
                f"Unsupported Python type in list for Spanner type inference: {type(value[0])}"
            )
        return spanner_v1.types.Type(
            code=spanner_v1.types.TypeCode.ARRAY,
            array_element_type=spanner_v1.types.Type(code=element_type_code),
        )

    type_code = PYTHON_TYPE_TO_SPANNER_TYPE_CODE.get(type(value))
    if not type_code:
        raise ValueError(
            f"Unsupported Python type for Spanner type inference: {type(value)}"
        )
    return spanner_v1.types.Type(code=type_code)

def insert_and_return(
    transaction: spanner_v1.transaction.Transaction,
    table_name: str,
    data_to_insert: dict,
    returning_columns: Optional[list[str]] = None,
    explicit_param_types: Optional[dict] = None,
):
    """
    Inserts a new row into the specified table using the provided transaction
    and returns the specified columns for the newly inserted row.

    Args:
        transaction: The active Spanner transaction object.
        table_name: The name of the table to insert into.
        data_to_insert: A dictionary where keys are column names and values are
                        the corresponding values to insert.
                        Example: {"name": "New Item", "quantity": 10}
        returning_columns: A list of column names to return after the insert.
                           These columns are typically auto-generated IDs or
                           columns with default values.
                           Example: ["item_id", "created_at"]
        explicit_param_types: Optional. A dictionary mapping parameter names (from data_to_insert)
                              to their Spanner types (e.g., {"quantity": param_types.INT64}).
                              If None, types will be inferred for basic Python types.

    Returns:
        A tuple containing the values of the columns specified in `returning_columns`
        for the newly inserted row. The order of values in the tuple matches the
        order in `returning_columns`.

    Raises:
        ValueError: If `data_to_insert` or `returning_columns` is empty, or if type
                    inference fails for a value and `explicit_param_types` isn't provided.
        RuntimeError: If the insert operation does not return the expected row.
    """
    if not table_name:
        raise ValueError("Table name must be provided.")
    if not data_to_insert:
        raise ValueError("Data to insert cannot be empty.")
    # if not returning_columns:
    #     raise ValueError("Returning columns list cannot be empty.")

    # --- : Filter out keys where the value is None ---
    data_to_insert = {k: v for k, v in data_to_insert.items() if v is not None}

    # Safety check: Ensure data_to_insert isn't empty after filtering
    if not data_to_insert:
        raise ValueError("Data to insert cannot be empty (all values were None).")
    # ----------------------------------------------------
    insert_cols = list(data_to_insert.keys())
    insert_val_placeholders = [f"@{col_name}" for col_name in insert_cols]

    # Construct the DML statement (no leading indentation for SQL)
    sql_statement = (
        f"INSERT INTO {table_name} ({', '.join(insert_cols)}) "
        f"VALUES ({', '.join(insert_val_placeholders)}) "
    )

    if returning_columns:
        sql_statement = (
            f"{sql_statement}"
            f"THEN RETURN {', '.join(returning_columns)}"
        )

    # Prepare parameters and their types
    params = data_to_insert.copy()  # Use a copy to avoid side effects
    param_types_dict = {}

    if explicit_param_types:
        param_types_dict.update(explicit_param_types)

    # Infer types for parameters not explicitly typed
    for col_name, value in data_to_insert.items():
        if col_name not in param_types_dict:  # Only infer if not already explicitly set
            try:
                param_types_dict[col_name] = infer_spanner_type(value)
            except ValueError as e:
                raise ValueError(
                    f"Type inference failed for column '{col_name}'. "
                    f"Please provide it in 'explicit_param_types'. Original error: {e}"
                )

    logger.info(
        f"Executing generic insert on {table_name} within transaction "
        #f"{transaction.transaction_id if transaction.transaction_id else ' (pending ID)'}"
    )
    logger.info(f"SQL: {sql_statement}")
    logger.info(f"Params: {params}")
    logger.info(f"Param Types: {param_types_dict}")

    results = list(
        transaction.execute_sql(
            sql_statement,
            params=params,
            param_types=param_types_dict,
        )
    )
    if not returning_columns:
        return

    if not results:
        raise RuntimeError(
            f"Insert operation into table '{table_name}' with data {data_to_insert} did not return any rows."
        )

    # `results` will be a list of tuples. For a single insert, it's a list with one tuple.
    # The tuple contains values in the order specified by `returning_columns`.
    logger.info(f"Successfully inserted into {table_name}: {results[0]}")
    return results[0]


def update_and_return(
    transaction: spanner_v1.transaction.Transaction,
    table_name: str,
    update_values: dict,
    where_conditions: dict,
    returning_columns: Optional[list[str]] = None,
    explicit_param_types: Optional[dict] = None,
):
    """
    Updates rows in the specified table using the provided transaction and returns
    specified columns for the updated rows.

    Args:
        transaction: The active Spanner transaction object.
        table_name: The name of the table to update.
        update_values: Dict of columns to update with their new values.
                       Example: {"quantity": 20, "status": "shipped"}
        where_conditions: Dict of columns and values to filter rows to update.
                          Example: {"order_id": 123}
        returning_columns: List of columns to return after update.
                           Example: ["order_id", "quantity", "status"]
        explicit_param_types: Optional dict mapping param names to Spanner types.

    Returns:
        List of tuples, each tuple contains values of `returning_columns` for an updated row.
        If `returning_columns` is None or empty, returns None.

    Raises:
        ValueError: If required parameters are missing or type inference fails.
        RuntimeError: If no rows are updated but returning_columns requested.
    """
    if not table_name:
        raise ValueError("Table name must be provided.")
    if not update_values:
        raise ValueError("update_values cannot be empty.")
    if not where_conditions:
        raise ValueError("where_conditions cannot be empty.")

    # Prepare SET clause
    set_cols = list(update_values.keys())
    set_placeholders = [f"{col} = @{col}" for col in set_cols]

    # Prepare WHERE clause
    where_cols = list(where_conditions.keys())
    where_placeholders = [f"{col} = @where_{col}" for col in where_cols]

    # Construct SQL statement
    sql_statement = (
        f"UPDATE {table_name} "
        f"SET {', '.join(set_placeholders)} "
        f"WHERE {' AND '.join(where_placeholders)} "
    )

    if returning_columns:
        sql_statement += f"RETURNING {', '.join(returning_columns)}"

    # Prepare params dict with distinct keys for SET and WHERE to avoid collision
    params = {}
    params.update(update_values)
    params.update({f"where_{k}": v for k, v in where_conditions.items()})

    # Prepare param types
    param_types_dict = explicit_param_types.copy() if explicit_param_types else {}

    # Infer types for params not explicitly typed
    for col_name, value in params.items():
        if col_name not in param_types_dict:
            try:
                param_types_dict[col_name] = infer_spanner_type(value)
            except ValueError as e:
                raise ValueError(
                    f"Type inference failed for parameter '{col_name}'. "
                    f"Please provide it in 'explicit_param_types'. Original error: {e}"
                )

    logger.info(f"Executing update on {table_name} within transaction")
    logger.info(f"SQL: {sql_statement}")
    logger.info(f"Params: {params}")
    logger.info(f"Param Types: {param_types_dict}")

    results = list(
        transaction.execute_sql(
            sql_statement,
            params=params,
            param_types=param_types_dict,
        )
    )

    if not returning_columns:
        return None

    if not results:
        raise RuntimeError(
            f"Update operation on table '{table_name}' with update_values {update_values} "
            f"and where_conditions {where_conditions} did not return any rows."
        )

    logger.info(f"Successfully updated {table_name}: {results}")
    return results


def delete_and_return(
    transaction: spanner_v1.transaction.Transaction,
    table_name: str,
    where_conditions: dict,
    returning_columns: Optional[list[str]] = None,
    explicit_param_types: Optional[dict] = None,
):
    """
    Deletes rows from the specified table using the provided transaction and returns
    specified columns for the deleted rows (if supported).

    Args:
        transaction: The active Spanner transaction object.
        table_name: The name of the table to delete from.
        where_conditions: Dict of columns and values to filter rows to delete.
                          Example: {"item_id": "abc-123"}
        returning_columns: List of columns to return after delete.
                           Example: ["item_id", "name"]
        explicit_param_types: Optional dict mapping param names to Spanner types.

    Returns:
        List of tuples, each tuple contains values of `returning_columns` for a deleted row.
        If `returning_columns` is None or empty, returns None.

    Raises:
        ValueError: If required parameters are missing or type inference fails.
        RuntimeError: If no rows are deleted but returning_columns requested.
    """
    if not table_name:
        raise ValueError("Table name must be provided.")
    if not where_conditions:
        raise ValueError("where_conditions cannot be empty.")

    # Prepare WHERE clause
    where_cols = list(where_conditions.keys())
    where_placeholders = [f"{col} = @{col}" for col in where_cols]

    # Construct SQL statement
    sql_statement = (
        f"DELETE FROM {table_name} "
        f"WHERE {' AND '.join(where_placeholders)} "
    )

    if returning_columns:
        sql_statement += f"RETURNING {', '.join(returning_columns)}"

    # Prepare params and param types
    params = where_conditions.copy()
    param_types_dict = explicit_param_types.copy() if explicit_param_types else {}

    for col_name, value in params.items():
        if col_name not in param_types_dict:
            try:
                param_types_dict[col_name] = infer_spanner_type(value)
            except ValueError as e:
                raise ValueError(
                    f"Type inference failed for parameter '{col_name}'. "
                    f"Please provide it in 'explicit_param_types'. Original error: {e}"
                )

    logger.info(f"Executing delete on {table_name} within transaction")
    logger.info(f"SQL: {sql_statement}")
    logger.info(f"Params: {params}")
    logger.info(f"Param Types: {param_types_dict}")

    results = list(
        transaction.execute_sql(
            sql_statement,
            params=params,
            param_types=param_types_dict,
        )
    )

    if not returning_columns:
        return None

    if not results:
        raise RuntimeError(
            f"Delete operation on table '{table_name}' with where_conditions {where_conditions} did not return any rows."
        )

    logger.info(f"Successfully deleted from {table_name}: {results}")
    return results
