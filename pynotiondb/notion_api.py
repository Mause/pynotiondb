import logging
from functools import lru_cache

import requests
from notion_client import Client

from .exceptions import NotionAPIError
from .mysql_query_parser import MySQLQueryParser

logger = logging.getLogger(__name__)


def format_type(s: str) -> dict:
    if s == "INT":
        return {"number": True}
    elif s == "VARCHAR":
        return {"rich_text": {}}
    elif s == "title":
        return {"title": {}}
    else:
        raise Exception(s)


class NotionAPI:
    SEARCH = "https://api.notion.com/v1/search"
    PAGES = "https://api.notion.com/v1/pages"
    UPDATE_PAGE = "https://api.notion.com/v1/pages/{}"
    DELETE_PAGE = "https://api.notion.com/v1/pages/{}"
    DATABASES = "https://api.notion.com/v1/databases/{}"
    QUERY_DATABASE = "https://api.notion.com/v1/databases/{}/query"
    DEFAULT_PAGE_SIZE_FOR_SELECT_STATEMENTS = 20
    token: str
    table_parent_page: str | None

    CONDITION_MAPPING = {
        "EQ": "equals",
        "GT": "greater_than",
        "LT": "less_than",
        "<=": "less_than_or_equal_to",
        ">=": "greater_than_or_equal_to",
    }

    def __init__(
        self,
        token: str,
        *,
        table_parent_page: str | None = None,
    ) -> None:
        self.token = token
        self.table_parent_page = table_parent_page
        self.DEFAULT_NOTION_VERSION = "2022-06-28"
        self.AUTHORIZATION = "Bearer " + self.token
        self.headers = {
            "Authorization": self.AUTHORIZATION,
            "Content-Type": "application/json",
            "Notion-Version": self.DEFAULT_NOTION_VERSION,
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.client = Client(auth=token)

    def request_helper(self, url: str, method: str = "GET", payload=None):
        response = self.session.request(method, url, json=payload)
        # response.raise_for_status()
        response = self.get_json(response)
        return response

    def get_json(self, response):
        if response.status_code >= 400:
            try:
                error_info = response.json()
                error_message = error_info.get("message", "Unknown Notion API Error")
                error_code = error_info.get("code", "Unknown Code")

            except Exception:
                error_message = "Unable to parse"
                error_code = "Unknown Code"

            raise NotionAPIError(
                f"Notion API Error ({response.status_code}): {error_message} ({error_code})"
            )

        else:
            return response

    def construct_payload_for_pages_creation(
        self, database_id: str, properties_data: dict
    ):
        json_data = {"parent": {"database_id": database_id}, "properties": {}}

        for data in properties_data["data"]:
            name = data["name"]
            if name == "number":
                json_data["properties"][data.get("property")] = {
                    "number": int(data.get("value"))
                }

            elif name in ["title", "rich_text"]:
                json_data["properties"][data.get("property")] = {
                    data.get("name"): [
                        {
                            "type": "text",
                            "text": {
                                "content": str(data.get("value")),
                            },
                            "plain_text": str(data.get("value")),
                        }
                    ]
                }
            elif name == "url":
                json_data["properties"][data.get("property")] = {
                    "url": str(data.get("value"))
                }
            else:
                logger.warn("Unsupported property type: %s", name)

        return json_data

    def get_table_header_info(self, database_id: str):
        response = self.request_helper(
            url=self.DATABASES.format(database_id), method="GET"
        )

        database_info = response.json()
        properties = database_info.get("properties", {})

        data = {}

        for property_name, property_info in properties.items():
            data[property_name] = {
                "id": property_info.get("id", ""),
                "name": property_info.get("type", ""),
                "type": property_info.get("name", ""),
            }

        return data

    def get_table_header(self, database_id: str):
        table_data = self.get_table_header_info(database_id)
        return tuple(table_data.keys())

    def get_all_database_info(self, cursor=None, page_size=20):
        payload = {
            "filter": {
                "value": "database",
                "property": "object",
            },
            "page_size": int(page_size),
        }

        if cursor:
            payload["start_cursor"] = cursor

        response = self.request_helper(url=self.SEARCH, method="POST", payload=payload)

        data = {"results": []}

        dbs_info = response.json()
        results = dbs_info.get("results", {})

        for result in results:
            data["results"].append(
                {
                    "id": result.get("id"),
                    "created_by": result.get("created_by"),
                    "last_edited_by": result.get("last_edited_by"),
                    "last_edited_time": result.get("last_edited_time"),
                    "title": result.get("title")[0].get("plain_text")
                    if len(result.get("title")) >= 1
                    else None,
                    "description": result.get("description")[0].get("plain_text")
                    if len(result.get("description")) >= 1
                    else None,
                    "properties": list(result.get("properties").keys()),
                }
            )

        data["has_more"] = dbs_info.get("has_more")
        data["next_cursor"] = dbs_info.get("next_cursor")
        data["previous_cursor"] = dbs_info.get("previous_cursor")

        return data

    @property
    @lru_cache()
    def databases(self):
        return {db["title"]: db["id"] for db in self.get_all_database_info()["results"]}

    def get_all_database(self):
        dbs = self.get_all_database_info()
        databases = [db.get("title") for db in dbs.get("results")]

        return tuple(databases)

    @staticmethod
    def __add_name_and_id_to_parsed_data_for_insert_statements(
        parsed_data, table_header
    ):
        for item in parsed_data["data"]:
            property = item["property"]
            assert property in table_header
            item["name"] = table_header[property]["name"]
            item["id"] = table_header[property]["id"]

        return parsed_data

    @staticmethod
    def __add_name_and_id_to_parsed_data_for_select_statements(
        parsed_data, table_header
    ):
        for condition in parsed_data.get("conditions", []):
            parameter = condition.get("parameter")
            assert parameter in table_header
            if parameter in table_header:
                condition["name"] = table_header[parameter]["name"]
                condition["id"] = table_header[parameter]["id"]
        return parsed_data

    @staticmethod
    def __add_name_and_id_to_parsed_data_for_update_statements(
        parsed_data, table_header
    ):
        set_values = parsed_data.get("set_values", [])
        updated_set_values = []

        for set_value in set_values:
            key = set_value.pop("key", None)

            if key and key in table_header:
                set_value.update(
                    {
                        "property": key,  # Changing 'key' to 'property'
                        "name": table_header[key]["name"],
                        "id": table_header[key]["id"],
                    }
                )

            updated_set_values.append(set_value)

        # Doing this so that using this we can later call construct payload function
        return {
            "table_name": parsed_data.get("table_name"),
            "data": updated_set_values,
            "where_clause": parsed_data.get("where_clause"),
        }

    @staticmethod
    def __generate_query(sql, val=None):
        if val is not None:
            query = sql.replace("%s", "'%s'")
            query = query % val

        else:
            query = sql

        return query

    def insert(self, query: str) -> None:
        parsed_data = MySQLQueryParser(query).parse()

        database_id = self.databases[parsed_data["table_name"]]

        table_header = self.get_table_header_info(database_id)

        parsed_data = self.__add_name_and_id_to_parsed_data_for_insert_statements(
            parsed_data, table_header
        )

        payload = self.construct_payload_for_pages_creation(database_id, parsed_data)

        return self.request_helper(self.PAGES, method="POST", payload=payload)

    def insert_many(self, sql, val) -> list:
        results = []
        for row in val:
            query = self.__generate_query(sql, row)

            parsed_data = MySQLQueryParser(query).parse()
            database_id = self.databases[parsed_data["table_name"]]
            table_header = self.get_table_header_info(database_id)
            parsed_data = self.__add_name_and_id_to_parsed_data_for_insert_statements(
                parsed_data, table_header
            )
            payload = self.construct_payload_for_pages_creation(
                database_id, parsed_data
            )
            results.append(
                self.request_helper(self.PAGES, method="POST", payload=payload)
            )
        return results

    def select(self, query):
        parsed_data = MySQLQueryParser(query).parse()

        database_id = self.databases[parsed_data["table_name"]]

        table_header = self.get_table_header_info(database_id)

        # We will need to add title, rich_text or number acc to the type of the property in the parsed_data which is parsed from the SELECT statement.
        # We only need to add name and id if there are condition in the statement

        if parsed_data.get("conditions") is not None:
            parsed_data = self.__add_name_and_id_to_parsed_data_for_select_statements(
                parsed_data, table_header
            )

        # If * is in the query that means it needs to have all the table headers so we need to use get_table_header()
        property_names = (
            parsed_data.get("columns", None)
            if parsed_data.get("columns")
            else self.get_table_header(database_id)
        )

        results = {
            "data": [],
            "next_cursor": "",
            "previous_cursor": None,
            "has_more": "",
        }

        page_size_dict = []

        if parsed_data.get("conditions") is not None:
            page_size_dict = [
                condition
                for condition in parsed_data.get("conditions", [])
                if condition.get("parameter") == "page_size" and condition is not None
            ]

        payload = {
            "page_size": page_size_dict[0].get("value")
            if len(page_size_dict) > 0
            else self.DEFAULT_PAGE_SIZE_FOR_SELECT_STATEMENTS,
            "filter": {"and": []},
        }

        all_conditions_except_page_size = parsed_data.get("conditions")

        if len(page_size_dict) > 0:
            all_conditions_except_page_size = [
                condition
                for condition in parsed_data.get("conditions", [])
                if condition.get("parameter") != "page_size"
            ]

        if all_conditions_except_page_size is not None:
            for condition in all_conditions_except_page_size:
                # LIKE is used to select the records which contains the word. So, other notion filter will be used

                if condition.get("operator") == "LIKE":
                    filter = {
                        "property": condition["parameter"],
                        "title": {"contains": condition["value"]},
                    }

                else:
                    print(condition)
                    filter = {
                        "property": condition["parameter"],
                        condition["name"]: {
                            self.CONDITION_MAPPING[condition["operator"]]: condition[
                                "value"
                            ]
                        },
                    }
                    print(filter)

                payload["filter"]["and"].append(filter)

        response = self.request_helper(
            self.QUERY_DATABASE.format(database_id), method="POST", payload=payload
        ).json()

        for entry in response["results"]:
            properties = entry["properties"]

            single_dict = {}

            for prop_name in property_names:
                prop_data = properties.get(prop_name, {})
                prop_type = prop_data.get("type", None)

                prop_value = None

                if prop_type and prop_type in prop_data:
                    if prop_type in ["title", "rich_text"]:
                        prop_value = (prop_data[prop_type] or [{}])[0].get(
                            "plain_text", ""
                        )

                    elif prop_type == "url":
                        prop_value = prop_data.get("url", None)
                    elif prop_type == "number":
                        prop_value = prop_data.get("number", None)

                    else:
                        prop_value = None

                single_dict[prop_name.lower()] = prop_value
                single_dict["id"] = entry["id"]
                single_dict["created_time"] = entry["created_time"]
                single_dict["last_edited_time"] = entry["last_edited_time"]

            # Check if any of the properties in the single_dict is empty
            if any(value for value in single_dict.values()):
                results["data"].append(single_dict)

        results["next_cursor"] = response.get("next_cursor", None)
        results["has_more"] = response.get("has_more", False)

        return results

    def update(self, query) -> None:
        parsed_data = MySQLQueryParser(query).parse()
        database_id = self.databases[parsed_data["table_name"]]

        table_header = self.get_table_header_info(database_id)

        parsed_data = self.__add_name_and_id_to_parsed_data_for_update_statements(
            parsed_data, table_header
        )

        select_statement_response = self.select(
            "SELECT * from {} {}".format(
                parsed_data["table_name"], parsed_data.get("where_clause")
            )
        )

        if not len(select_statement_response["data"]) >= 0:
            raise ValueError("No Data Found")

        for entry in select_statement_response["data"]:
            payload = self.construct_payload_for_pages_creation(
                database_id, parsed_data
            )

            # We don't want "parent" key in the payload
            payload.pop("parent")

            self.request_helper(
                url=self.UPDATE_PAGE.format(entry["id"]),
                method="PATCH",
                payload=payload,
            )

    def create(self, query: str) -> None:
        if not self.table_parent_page:
            raise Exception("Parent for new tables must be specified")
        parsed_data = MySQLQueryParser(query).parse()
        props = {col: format_type(typ) for col, typ in parsed_data["columns"].items()}
        return self.client.databases.create(
            title=[{"text": {"content": parsed_data["table_name"]}}],
            parent={"database_id": self.table_parent_page},
            properties=props,
        )

    def delete(self, query) -> None:
        parsed_data = MySQLQueryParser(query).parse()

        select_statement_response = self.select(
            "SELECT * from {} {}".format(
                parsed_data["table_name"], parsed_data.get("where_clause")
            )
        )

        if not len(select_statement_response["data"]) >= 0:
            raise ValueError("No Data Found")

        for entry in select_statement_response["data"]:
            payload = {
                "in_trash": True,
            }

            self.request_helper(
                url=self.DELETE_PAGE.format(entry["id"]),
                method="PATCH",
                payload=payload,
            )

    def execute(self, sql, val=None):
        if val is not None and isinstance(val, list):
            query = sql
            to_execute_many = True

        else:
            query = self.__generate_query(sql, val)
            to_execute_many = False

        parser = MySQLQueryParser(query)

        can_continue, to_do = parser.check_statement()

        if can_continue:
            if to_do == "insert":
                self.insert_many(sql, val) if to_execute_many else self.insert(query)

            elif to_do == "select":
                return self.select(query)

            elif to_do == "update":
                self.update(query)

            elif to_do == "delete":
                self.delete(query)

            elif to_do == "create":
                return self.create(query)

            else:
                raise ValueError("Unsupported operation")

        else:
            raise ValueError(
                "Invalid SQL statement or type of statement not implemented"
            )
