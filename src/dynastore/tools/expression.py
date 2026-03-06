"""
Shared tools for SQL-like expression evaluation in-memory.
"""
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

def evaluate_sql_condition(condition: str, properties: Dict[str, Any], params: Dict[str, Any] = {}) -> bool:
    """
    Evaluates a SQL-like condition (from CQL2) against a set of properties in-memory.
    Uses Python's eval() in a controlled environment.
    """
    if not condition:
        return True

    try:
        eval_expr = condition
        
        # 1. Replace bind parameters with actual values
        for param_name, param_value in params.items():
            if isinstance(param_value, str):
                eval_expr = eval_expr.replace(f":{param_name}", f"'{param_value}'")
            else:
                eval_expr = eval_expr.replace(f":{param_name}", str(param_value))

        # 2. Replace column references with property values
        # We sort by length descending to avoid partial matches (e.g., 'id' vs 'external_id')
        sorted_props = sorted(properties.keys(), key=len, reverse=True)
        for prop_name in sorted_props:
            prop_value = properties[prop_name]
            if prop_value is None:
                eval_expr = eval_expr.replace(prop_name, "NULL")
            elif isinstance(prop_value, str):
                eval_expr = eval_expr.replace(prop_name, f"'{prop_value}'")
            else:
                eval_expr = eval_expr.replace(prop_name, str(prop_value))

        # 3. Convert SQL operators to Python
        eval_expr = eval_expr.replace(" = ", " == ")
        eval_expr = eval_expr.replace(" AND ", " and ")
        eval_expr = eval_expr.replace(" OR ", " or ")
        eval_expr = eval_expr.replace(" NOT ", " not ")
        eval_expr = eval_expr.replace(" IS NULL", " is None")
        eval_expr = eval_expr.replace(" IS NOT NULL", " is not None")
        eval_expr = eval_expr.replace("NULL", "None")

        # 4. Evaluate safely
        return bool(eval(eval_expr, {"__builtins__": {}}, {}))

    except Exception as e:
        logger.warning(f"Failed to evaluate SQL condition '{condition}': {e}")
        return False
