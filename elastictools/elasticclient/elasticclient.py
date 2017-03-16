from elasticsearch import Elasticsearch, RequestsHttpConnection
import json
from enum import Enum

connections = {}


class AuthType(Enum):
    BY_LOGIN = "byLogin"
    BY_HOST = "byHost"



class Response:
    def __init__(self, request):
        class Getters(object): pass
        self.request = copy.deepcopy(request)
        self.request_body = self.request["body"]
        self.getters_dict = {}
        self.getters = Getters()
        self.getter = None
        def getter_factory(key):
            def result(*args, **kwargs):
                return self.request["getters"][key](self.response_body, *args, **kwargs)
            return result
        for getter in request["getters"]:
            self.getters_dict[getter] = getter_factory(getter)
            setattr(self.getters, getter, self.getters_dict[getter])
        self.response_body = None


class Credentials:
    def __init__(self, p_json, name):
        self.name = name
        self.host = p_json["host"]
        self.auth_type = p_json["auth_type"]
        if self.auth_type == AuthType.BY_LOGIN.value:
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
    f.close()
    for option in plain:
        x = Credentials(plain[option], option)
        connections[x.name] = x

    if "default" not in connections:
        connections["default"] = connections[next(iter(connections))]
    else:
        print("Warning: pre-defined \"default\" credentials found. Reassinging default in runtime will override them")


def assign_default(name):
    connections["default"] = connections[name]


def search(connection_name="default", **kwargs):
    connection = connections[connection_name]
    if connection.connection is None:
        connection.get_connection()
    result = Response(kwargs["body"])
    if kwargs["body"] is not None:
        kwargs["body"] = kwargs["body"]["body"]
    result.response_body = connection.connection.search(**kwargs)
    return result



