from dateutil.parser import parse
from datetime import timedelta
import elasticsearch


def request(query, fieldlist=None, sorting=None, **aggs):
    aggs_bodys = {key: aggs[key]["body"] for key in aggs}
    body = {
        **(
            {
                "query": query
            } if len(query) > 0 else {}
        ),
        **(
            {
                "aggs": aggs_bodys
            } if len(aggs_bodys) > 0 else {}
        ),
        **(
            {
                "_source": fieldlist
            } if fieldlist is not None else {}
        ),
        **(
            {
                "sort":
                {
                    skey:
                    {
                        "order": sorting[skey]
                    } for skey in sorting
                }
            } if sorting is not None else {}
        )
    }
    def children_getter(p_getter, key):
        def deeper_getter(response_body):
            return p_getter(response_body["aggregations"][key])
        return deeper_getter
    getters = {key: children_getter(aggs[aggr]["getters"][key], aggr) for aggr in aggs for key in aggs[aggr]["getters"]}
    return {"body": body, "getters": getters}


def filter_query(flt):
    return {
        "filtered":
        {
            "filter": flt
        }
    }


def flt_and(*args):
    return {
        "bool":
        {
            "must": args
        }
    }


def flt_exc(*args):
    return {
        "bool": {
            "must_not": args
        }
    }


def flt_or(*args):
    return {
        "bool": {
            "should": args
        }
    }


def flt_eq(field, value):
    if isinstance(value, list):
        return {"terms": {field: value}}
    elif isinstance(value, str) and value[-1] == '*':
        return {"prefix": {field: value[:-1]}}
    return {"term": {field: value}}


def flt_exists(field):
    return {"exists": {"field": field}}


def flt_range(field, left=None, right=None, left_is_strict=False, right_is_strict=False):
    return {"range": {field: {**({"gt" + ("" if left_is_strict else "e"): left} if left is not None else {}), **({ "lt" + ("" if right_is_strict else "e"): right} if right is not None else {})}}}


def agg(body):
    # getter = body["getters"]["self"]
    aggs_bodys = {key: body["aggs"][key]["body"] for key in body["aggs"]}
    getters = {getter: body["children_getter"](body["aggs"][aggr]["getters"][getter], aggr) for aggr in body["aggs"] for getter in body["aggs"][aggr]["getters"] if getter != "self"}
    getters.update(body["getters"])

    body = {**body["body"], **({"aggs": aggs_bodys} if len(aggs_bodys) > 0 else {})}
    return {"body": body, "getters": getters}


def i_can_has_children(func):
    def decorated_agg(*args, **kwargs):
        return agg(func(*args, **kwargs))
    return decorated_agg


@i_can_has_children
def agg_filter(flt, getter_name=None, **kwargs):
    def getter(response_body):
        return response_body["value"]

    def children_getter(getter, key):
        def deeper_getter(response_body):
            return getter(response_body[key])
        return deeper_getter

    body = {"filter": flt}
    getters = {"self": getter}
    # children_getters = {"children_getter": children_getter}
    if getter_name is not None:
        getters[getter_name] = getter
    return {"body": body, "getters": getters, "children_getter": children_getter, "aggs": kwargs}


def agg_cardinality(field, getter_name=None):
    def getter(response_body):
        return response_body["value"]
    body = {"cardinality": {"field": field}}
    getters = {"self": getter}
    if getter_name is not None:
        getters[getter_name] = getter

    return {"body": body, "getters": getters}


@i_can_has_children
def agg_terms(field, script=False, size=10000, min_doc_count=None, order=None, getter_doc_count=None, getter_key=None, **kwargs):
    getters = generate_getter({}, getter_doc_count, "doc_count")
    getters = generate_getter(getters, getter_key, "key")
    if order is None:
        order = {"_count": "desc"}
    return {"terms": {**{"script" if script else "field": field, "size": size},
                      **({"min_doc_count": min_doc_count} if min_doc_count is not None else {}),
                      **({"order": order} if order is not None else {})}}


def agg_sum(field, script=False):
    return {"sum": {("script" if script else "field"): field}}


def agg_avg(field, script=False):
    return {"avg": {("script" if script else "field"): field}}


def agg_min(field, script=False):
    return {"min": {("script" if script else "field"): field}}


def agg_max(field, script=False):
    return {"max": {("script" if script else "field"): field}}


@i_can_has_children
def agg_top_hits(size, sorting=None, fields=None):
    return {
        "top_hits":
        {
            "size": size,
            **(
                {
                    "sort":
                    {
                        skey:
                        {
                            "order": sorting[skey]
                        } for skey in sorting
                    }
                } if sorting is not None else {}
            ),
            **(
                {
                    "_source":
                    {
                        "includes": fields
                    }
                } if fields is not None else {}
            )
        }
    }


def agg_histogram(field, interval, date_histogram=False):
    return {"date_histogram" if date_histogram else "histogram": {"field": field, "interval": interval}}


def agg_extended_stats(field, script=False,  sigma=2):
    return {"extended_stats": {("script" if script else "field"): field, "sigma": sigma}}


def generate_getter(getters, getter_name, field_name, buckets_name="buckets"):
    def getter(response_body):
        return response_body[buckets_name][field_name]
    if getter_name is not None:
        getters.update({
            getter_name: getter
        })
    return getters


req = request({}, test=agg_filter(
    flt_eq("env", "prod"),
    unique=agg_cardinality("p", getter_name="MASHEN'KA")
))
y = req["body"]
# x = req["aggs"]["test"]["getters"]["MASHEN'KA"]
json_output = {
   "took": 311,
   "timed_out": False,
   "_shards": {
      "total": 31,
      "successful": 31,
      "failed": 0
   },

   "aggregations": {
      "test": {
         "doc_count": 17511389,
         "unique": {
            "value": 1616002
         }
      }
   }
}
x = req["getters"]["MASHEN'KA"](json_output)
print(req)
