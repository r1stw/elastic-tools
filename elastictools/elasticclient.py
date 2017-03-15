from elasticsearch import Elasticsearch, RequestsHttpConnection
import json
from enum import Enum

connections = {}


class AuthType(Enum):
    BY_LOGIN = "bylogin",
    BY_HOST = "byHost"


class Credentials:
    def __init__(self, p_json, name):
        self.name = name
        self.host = p_json["host"]
        self.auth_type = AuthType[p_json["auth_type"]]
        if self.auth_type == AuthType.BY_LOGIN:
            self.login = p_json["login"]
            self.password = p_json["password"]
        self.connection = None

    def get_connection(self):
        if self.auth_type == AuthType.BY_LOGIN:
            self.connection = Elasticsearch(self.host,
                                            connection_class=RequestsHttpConnection,
                                            http_auth=(self.login, self.password), use_ssl=False,
                                            timeout=120)
        else:
            self.connection = Elasticsearch(self.host, timeout=60)


def get_credentials(path_to_credentials_file):
    global connections
    connections = {}
    f = open(path_to_credentials_file, 'r')
    plain = json.load(f)
    for option in plain:
        x = Credentials(plain[option], option)
        connections[x.name] = x

    if "default" not in connections:
        connections["default"] = next(connections)
    else:
        print("Warning: pre-defined \"default\" credentials found. Reassinging default in runtime will override them")


def assign_default(name):
    connections["default"] = connections[name]


def search(connection_name="default", **kwargs):
    connection = connections[connection_name]
    if connection.connection is None:
        connection.get_connection()
    return connection.connection.search(**kwargs)



