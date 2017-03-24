from elasticsearch import Elasticsearch, RequestsHttpConnection
import json
from enum import Enum
import copy

connections = {}


class AuthType(Enum):
    BY_LOGIN = "byLogin"
    BY_HOST = "byHost"


class NotExecutedException(Exception):
    pass


class Request:
    def __init__(self, **es_search_args):
        class Getters(object): pass

        def getter_factory(key):
            def result(*args, **kwargs):
                return self.request["getters"][key](self.response_body, *args, **kwargs)
            return result

        self.request = copy.deepcopy(es_search_args["body"])
        self.request_body = self.request["body"]
        self.es_search_args = copy.deepcopy(es_search_args)
        self.es_search_args["body"] = self.request_body

        self.getters_dict = {}
        self.getters = Getters()

        for getter in self.request["getters"]:
            self.getters_dict[getter] = getter_factory(getter)
            setattr(self.getters, getter, self.getters_dict[getter])
        self.response_body = None
        self.executed = False
        self.axis_table = []

    def keys_iter(self):
        if not self.executed:
            raise NotExecutedException
        self.axis_table = []
        self.__fill_axis_table(self.request["axis"](self.response_body), ())

    def __keys_iter(self, obj):
        if len(obj):
            for child_key in obj:
                for temp in self.__keys_iter(obj[child_key]):
                    if temp is not None:
                        yield (child_key, *temp)
                    else:
                        yield tuple(child_key)
        else:
            yield

    def __fill_axis_table(self, obj, tuple_draft):
        if len(obj) > 0:
            for key in obj:
                tuple_draft_temp = (*tuple_draft, key)
                self.__fill_axis_table(obj[key], tuple_draft_temp)
        else:
            self.axis_table.append(tuple_draft)

    def line_iterator(self):
        if not self.executed:
            raise NotExecutedException
        for addr in self.axis_table:
            yield {getter: value(*addr) for getter, value in self.getters_dict.items()}

    def execute(self, connection_name="default", **kwargs):
        if len(kwargs) == 0:
            kwargs = self.es_search_args
        connection = connections[connection_name]
        if connection.connection is None:
            connection.get_connection()
        self.response_body = connection.connection.search(**kwargs)
        self.executed = True


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
    result = Request(kwargs["body"])
    if kwargs["body"] is not None:
        kwargs["body"] = kwargs["body"]["body"]
    result.response_body = connection.connection.search(**kwargs)
    return result







