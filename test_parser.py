from pytest import mark
from pynotiondb.mysql_query_parser import MySQLQueryParser


@mark.parametrize(
    "sql",
    [
        "SELECT * FROM table",
        "SELECT column1, column2 FROM table WHERE column1 = 'value'",
        "INSERT INTO table1 (column1, column2) VALUES ('value1', 'value2')",
        "UPDATE table SET column1 = 'new_value' WHERE column2 = 'value2'",
        "DELETE FROM table WHERE column1 = 'value1';",
        "SELECT * FROM table1 WHERE col1=1 AND col2='text' OR col3 IS NULL;",
    ],
)
def test_sql_parser(sql: str, snapshot):
    parser = MySQLQueryParser(sql)
    ok, typ = parser.check_statement()
    assert ok

    snapshot.assert_match(parser.parse())
