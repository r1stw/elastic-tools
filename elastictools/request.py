from dateutil.parser import parse
from datetime import timedelta


def request(query, fieldlist=None, sorting=None, **aggs):
    return {
        **(
            {
                "query": query
            } if len(query) > 0 else {}
        ),
        **(
            {
                "aggs": aggs
            } if len(aggs) > 0 else {}
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


def agg(body, **aggs):
    return {**body, **({"aggs": aggs} if len(aggs) > 0 else {})}


def agg_filter(flt):
    return {"filter": flt}


def agg_cardinality(field):
    return {"cardinality": {"field": field}}


def agg_terms(field, script=False, size=10000, min_doc_count=None, order=None):
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
