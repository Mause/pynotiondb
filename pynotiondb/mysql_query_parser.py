from sqlglot import parse
from sqlglot.expressions import (
    EQ,
    And,
    Delete,
    Expression,
    Insert,
    Select,
    Star,
    Update,
)


class MySQLQueryParser:
    def __init__(self, statement: str) -> None:
        self.statement = parse(statement)[0]

    @staticmethod
    def _process_string(input_string: list[Expression]) -> list[str]:
        return [hing.this for hing in input_string]

    def extract_insert_statement_info(self) -> dict:
        assert isinstance(self.statement, Insert)
        match: Insert = self.statement

        table_name = match.this.this.this.this
        prop_string = match.this.expressions
        values_string = match.expression.expressions[0].expressions

        properties = self._process_string(prop_string)
        values = self._process_string(values_string)

        data = []

        for index in range(len(properties)):
            data.append({"property": properties[index], "value": values[index]})

        if len(properties) > len(values):
            raise Exception(
                "The number of properties specified in the INSERT statement is "
                "larger than the number of values. Please ensure that the number "
                "of properties matches the number of values to correctly assign "
                "each property a corresponding value."
            )

        elif len(values) > len(properties):
            raise Exception(
                "The number of values provided in the INSERT statement is larger "
                "than the number of properties. Please ensure that the number of "
                "values matches the number of properties in order to correctly "
                "map each value to its corresponding property."
            )

        return {"table_name": table_name, "data": data}

    def extract_select_statement_info(self) -> dict:
        assert isinstance(self.statement, Select)
        match: Select = self.statement

        table_name = match.args.get("from").this.this.this

        exprs = match.expressions
        columns = (
            None
            if len(exprs) == 1 and isinstance(exprs[0], Star)
            else [res.this for res in self._process_string(match.expressions) if res]
        )

        conditions_str = match.args.get("where")
        conditions = self.unwrap_where(conditions_str)

        return {
            "table_name": table_name,
            "columns": columns,
            "conditions": conditions if len(conditions) != 0 else None,
        }

    def unwrap_where(self, conditions_str) -> list[dict]:
        conditions = []
        if conditions_str:
            if isinstance(conditions_str.this, And):
                conditions.append(self.parse_condition(conditions_str.this.this))
                conditions.append(self.parse_condition(conditions_str.this.expression))
            else:
                conditions.append(self.parse_condition(conditions_str.this))
        return conditions

    def parse_condition(self, op: Expression) -> dict:
        operator = type(op).__name__
        key = op.this.this.this
        value = op.expression.this

        key = key.strip()
        operator = operator.strip()
        return {
            "parameter": key,
            "operator": operator,
            "value": int(value) if value.isdigit() else value,
        }

    def extract_update_statement_info(self) -> dict:
        match: Update = self.statement

        table_name = match.this.this.this
        set_values_str = match.expressions
        where_clause = match.args.get("where")

        set_values = self.extract_set_values(set_values_str)

        return {
            "table_name": table_name,
            "set_values": set_values,
            "where_clause": self.unwrap_where(where_clause),
        }

    def extract_delete_statement_info(self) -> dict:
        match: Delete = self.statement

        table_name = match.this.this.this
        where_clause = match.args.get("where")

        return {"table_name": table_name, "where_clause": where_clause}

    def extract_set_values(self, set_values_str: list[EQ]) -> list[dict]:
        set_values = []
        # Split by 'AND', but not within quotes
        pairs = set_values_str
        for pair in pairs:
            # Find the position of the first '=' outside quotes

            key = pair.this.this.this
            value = pair.expression.this

            # Handle numeric values
            if value.isdigit():
                value = int(value)
            elif value.replace(".", "", 1).isdigit():
                value = float(value)

            set_values.append({"key": key, "value": value})
        return set_values

    def parse(self) -> dict:
        if isinstance(self.statement, Insert):
            return self.extract_insert_statement_info()

        if isinstance(self.statement, Select):
            return self.extract_select_statement_info()

        if isinstance(self.statement, Update):
            return self.extract_update_statement_info()

        if isinstance(self.statement, Delete):
            return self.extract_delete_statement_info()

        raise ValueError("Invalid SQL statement")

    def check_statement(self) -> tuple[bool, str]:
        if isinstance(self.statement, Insert):
            return True, "insert"
        if isinstance(self.statement, Select):
            return True, "select"
        if isinstance(self.statement, Update):
            return True, "update"
        if isinstance(self.statement, Delete):
            return True, "delete"

        return False, "unknown"
