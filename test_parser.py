import json

from pytest import mark
from respx import MockRouter

from pynotiondb import NotionAPI
from pynotiondb.mysql_query_parser import MySQLQueryParser


@mark.parametrize(
    "sql",
    [
        "SELECT * FROM table1",
        "SELECT column1, column2 FROM table1 WHERE column1 = 'value'",
        "INSERT INTO table1 (column1, column2) VALUES ('value1', 'value2')",
        "UPDATE table SET column1 = 'new_value' WHERE column2 = 'value2'",
        "DELETE FROM table1 WHERE column1 = 'value1';",
        "SELECT * FROM table1 WHERE column1=1 AND column2='text' OR column3 IS NULL;",
        "SELECT *, agg_list(column) FROM table GROUP BY column2 LIMIT 10 OFFSET 5;",
    ],
)
def test_sql_parser(sql: str, snapshot):
    parser = MySQLQueryParser(sql)
    ok, typ = parser.check_statement()
    assert ok

    snapshot.assert_match(parser.parse())


create_sql = "CREATE TABLE table1 (id int);"


def test_create(snapshot):
    parser = MySQLQueryParser(create_sql)
    ok, typ = parser.check_statement()
    assert ok

    snapshot.assert_match(parser.parse())


def test_notion(snapshot):
    with MockRouter(base_url="https://api.notion.com/v1") as req:
        call = req.post("/databases").respond(200, json={})
        notion = NotionAPI("", {"table1": "table1"})
        notion.execute(create_sql)

        assert call.called
        assert snapshot == json.loads(req.calls.last.request.content)
