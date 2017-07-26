def add_getter(getters, getter_name, field_name, additional_level=None):
    if getter_name is None:
        return

    if additional_level is None:
        def getter(response_body, *args, **kwargs):
            return response_body[field_name]
    else:
        def getter(response_body, *args, **kwargs):
            return response_body[additional_level][field_name]
        pass

    getters.update({getter_name: getter})
    pass


def aggregation_linker(aggregation):
    getters = aggregation["getters"]
    sub_aggs_bodys = {}

    if "sub_aggs" in aggregation:
        sub_aggs_bodys = {key: aggregation["sub_aggs"][key]["body"] for key in aggregation["sub_aggs"]}
        for aggr_key, aggr_value in aggregation["sub_aggs"].items():
            for getter_key, getter_value in aggr_value["getters"].items():
                getters.update(aggregation["getter_updater"](getter_value, aggr_key, getter_key))
                pass
            pass
        pass

    if "axis_maker" in aggregation:
        axis = aggregation["axis_maker"]()
        if "sub_aggs" in aggregation:
            for child in aggregation["sub_aggs"]:
                if "axis" in aggregation["sub_aggs"][child] and aggregation["sub_aggs"][child]["axis"] is not None:
                    axis = aggregation["axis_maker"](aggregation["sub_aggs"][child]["axis"], child)
                    break
                pass
            pass
        pass
    else:
        axis = None

    body = {**aggregation["body"], **({"aggs": sub_aggs_bodys} if len(sub_aggs_bodys) > 0 else {})}
    return {"body": body, "getters": getters, **({"axis": axis} if axis is not None else {})}


def bucket_agg(func):
    def decorated_agg(*args, **kwargs):
        return aggregation_linker(func(*args, **kwargs))
    return decorated_agg


def plain_multi_bucket_getter_updater(getter, key, getter_name):
    def deeper_getter(response_body, *args, **kwargs):
        if isinstance(response_body["buckets"], list):
            return [getter(bucket[key], *args, **kwargs) for bucket in response_body["buckets"]]
        else:
            return [getter(bucket[key], *args, **kwargs) for bucket in response_body["buckets"].values()]
    return {getter_name: deeper_getter}


def axis_multi_bucket_getter_updater(getter, key, getter_name):
    def deeper_getter(response_body, bucket_id, *args, **kwargs):
        if len(response_body["buckets"]) == 0:
            return None
        if isinstance(response_body["buckets"], dict):
            bucket_id = list(response_body["buckets"].keys())[bucket_id]
        return getter(response_body["buckets"][bucket_id][key], *args, **kwargs)
    return {getter_name: deeper_getter}


def create_per_bucket_getter_updater(bucket_key):
    def per_bucket_getter_updater(getter, key, getter_name):
        def deeper_getter(response_body, *args, **kwargs):
            if isinstance(response_body["buckets"], dict):
                return getter(response_body["buckets"][bucket_key][key], *args, **kwargs)
            else:
                for index, bucket in enumerate(response_body["buckets"]):
                    if bucket["key"] == bucket_key:
                        b_id = index
                        return getter(response_body["buckets"][b_id][key], *args, **kwargs)
                    pass
                return None
        return {getter_name + "_" + str(bucket_key): deeper_getter}
    return per_bucket_getter_updater


def split_multi_bucket_getter_updater_factory(bucket_keys):
    def split_multi_bucket_getter_updater(getter, key, getter_name):
        ret = {}
        for bucket_key in bucket_keys:
            ret = {**ret, **create_per_bucket_getter_updater(bucket_key)(getter, key, getter_name)}
            pass
        return ret
    return split_multi_bucket_getter_updater


def multi_bucket_axis_maker(next_axis=None, next_axis_name=None):
    def axis(response_body):
        if len(response_body["buckets"]) == 0:
            return {0: {}}
        if next_axis_name is None:
            return {i: {} for i in range(len(response_body["buckets"]))}
        return {i: next_axis(response_body["buckets"][i][next_axis_name]) for i in range(len(response_body["buckets"]))}

    return axis


def percentile_axis_maker(next_axis=None, next_axis_name=None):
    def axis(response_body):
        if len(response_body["values"]) == 0:
            return {0: {}}
        return {i: {} for i in response_body["values"].keys()}

    return axis


def single_bucket_getter_updater(getter, key, getter_name):
    def deeper_getter(response_body, *args, **kwargs):
        return getter(response_body[key], *args, **kwargs)
    return {getter_name: deeper_getter}


def single_bucket_axis_maker(next_axis=None, next_axis_name=None):
    if next_axis_name is None:
        return None
    else:
        def axis(response_body):
            return next_axis(response_body[next_axis_name])
        return axis


def request(query=None, fieldlist=None, sorting=None, **aggs):
    """
    Creates core request body. \n
    :param query: Pure JSON query body. Empty by default. \n
    :param fieldlist: List of fields to write in "_source". Empty by default, which means that all source fields will be included in response hits \n
    :param sorting: Sorting dict, {"field1": "asc", "field2": "desc} for example \n
    :param aggs: Aggregation objects provided by agg functions \n
    :return: {"body": %plain json request%, "getters": %dictionary of named getters%} \n
    """
    if query is None:
        query = {}
        pass
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

    def getter_updater(p_getter, key):
        def deeper_getter(response_body, *args, **kwargs):
            return p_getter(response_body["aggregations"][key], *args, **kwargs)
        return deeper_getter
    getters = {key: getter_updater(aggs[aggr]["getters"][key], aggr) for aggr in aggs for key in aggs[aggr]["getters"]}
    axis = lambda x: {}
    for aggregation in aggs:
        if "axis" in aggs[aggregation]:
            axis = getter_updater(aggs[aggregation]["axis"], aggregation)
            break
        pass
    return {"body": body, "getters": getters, "axis": axis}


##############################################################################################
# QUERIES                                                                                    #
##############################################################################################


def query_filter(*filters):
    return {
        "bool":
        {
            "filter": filters
        }
    }


##############################################################################################
# FILTERS                                                                                    #
##############################################################################################


def flt_and(*filters):
    return {
        "bool":
        {
            "filter": filters
        }
    }


def flt_not(*filters):
    return {
        "bool": {
            "must_not": filters
        }
    }


def flt_or(*filters):
    return {
        "bool": {
            "should": filters
        }
    }


def flt_eq(field, value):
    if isinstance(value, list):
        return {"terms": {field: value}}
    elif isinstance(value, str) and value[-1] == '*' and value.count("*") == 1 and value.count("?") == 0:
        return {"prefix": {field: value[:-1]}}
    elif isinstance(value, str) and value.count("*") + value.count("?") > 0:
        return {"wildcard": {field: value}}
    return {"term": {field: value}}


def flt_exists(field):
    return {"exists": {"field": field}}


def flt_range(field, left=None, right=None, left_is_strict=False, right_is_strict=False):
    return {"range": {field: {**({"gt" + ("" if left_is_strict else "e"): left} if left is not None else {}), **({ "lt" + ("" if right_is_strict else "e"): right} if right is not None else {})}}}


def flt_type(field, typename):
    return {"type": {field: typename}}


def flt_nested(path, *filters):
    return {"nested": {"path": path, "query": {"bool": {"filter": filters}}}}


def flt_has_child(doctype, min_children=0, max_children=10000, *filters):
    return{
        "type": doctype,
        "query": {
            "bool":
            {
                "filter": filters
            },
            "min_children": min_children,
            "max_children": max_children,
            "score_mode": "none"
        },
    }


def flt_has_parent(doctype, *filters):
    return{
        "type": doctype,
        "query": {
            "bool":
            {
                "filter": filters
            },
            "score_mode": False
        },
    }


##############################################################################################
# BUCKET AGGS                                                                                #
##############################################################################################


@bucket_agg
def agg_filter(flt, getter=None, **kwargs):
    getters = {}
    add_getter(getters, getter, "doc_count")

    body = {"filter": flt}

    return {"body": body, "getters": getters, "getter_updater": single_bucket_getter_updater, "sub_aggs": kwargs, "axis_maker": single_bucket_axis_maker}


@bucket_agg
def agg_nested(path, getter=None, **kwargs):
    getters = {}
    add_getter(getters, getter, "doc_count")

    body = {"nested": {"path": path}}

    return {"body": body, "getters": getters, "getter_updater": single_bucket_getter_updater, "sub_aggs": kwargs, "axis_maker": single_bucket_axis_maker}


@bucket_agg
def agg_reverse_nested(getter=None, **kwargs):
    getters = {}
    add_getter(getters, getter, "doc_count")

    body = {"reverse_nested": {}}

    return {"body": body, "getters": getters, "getter_updater": single_bucket_getter_updater, "sub_aggs": kwargs,
            "axis_maker": single_bucket_axis_maker}


@bucket_agg
def agg_terms(field, script=False, size=10000, min_doc_count=None, order=None, getter_doc_count=None, getter_key=None, is_axis=True, **kwargs):
    getters = {}
    add_getter(getters, getter_doc_count, "doc_count")
    add_getter(getters, getter_key, "key")

    def getter_factory(key, bucket_id=None):
        def result_plain(response_body, *args, **kwargs2):
            return [getters[key](bucket) for bucket in response_body["buckets"]]

        def result_axis(response_body, bucket_id, *args, **kwargs2):
            if len(response_body["buckets"]) > 0:
                return getters[key](response_body["buckets"][bucket_id])
            else:
                return None

        def split_factory(bucket_key):
            def result_split(response_body, *args, **kwargs2):
                b_id = None
                for index, bucket in enumerate(response_body["buckets"]):
                    if bucket["key"] == bucket_key:
                        b_id = index
                        pass
                    pass
                if b_id is None:
                    return None
                return getters[key](response_body["buckets"][b_id])
            return result_split

        if isinstance(is_axis, list):
            return {key + "_" + str(key2): split_factory(key2) for key2 in is_axis}
            pass
        elif is_axis:
            return {key: result_axis}
        else:
            return {key: result_plain}

    getters_new = {}
    for getter in getters:
        getters_new = {**getters_new, **getter_factory(getter)}

    if isinstance(is_axis, list):
        getter_updater = split_multi_bucket_getter_updater_factory(is_axis)
    elif is_axis:
        getter_updater = axis_multi_bucket_getter_updater
    else:
        getter_updater = plain_multi_bucket_getter_updater

    body = {"terms": {
        **{"script" if script else "field": field, "size": size},
        **({"min_doc_count": min_doc_count} if min_doc_count is not None else {}),
        **({"order": order} if order is not None else {})}
    }
    if is_axis and not isinstance(is_axis, list):
        return {"body": body, "getters": getters_new, "getter_updater": getter_updater, "sub_aggs": kwargs, "axis_maker": multi_bucket_axis_maker}
    else:
        return {"body": body, "getters": getters_new, "getter_updater": getter_updater, "sub_aggs": kwargs}


@bucket_agg
def agg_histogram(field, interval, getter_doc_count=None, getter_key=None, getter_key_as_string=None, date_histogram=False, is_axis=True, **kwargs):
    getters = {}
    add_getter(getters, getter_doc_count, "doc_count")
    add_getter(getters, getter_key, "key")
    add_getter(getters, getter_key_as_string, "key_as_string")
    if not date_histogram and getter_key_as_string is not None:
        raise ValueError("getter_key_as_string cannot be specified in usual histogram")

    def getter_factory(key):
        def result_plain(response_body, *args, **kwargs2):
            return [getters[key](bucket) for bucket in response_body["buckets"]]

        def result_axis(response_body, bucket_id, *args, **kwargs2):
            if len(response_body["buckets"]) > 0:
                return getters[key](response_body["buckets"][bucket_id])
            else:
                return None

        def split_factory(bucket_key):
            def result_split(response_body, *args, **kwargs2):
                b_id = None
                for index, bucket in enumerate(response_body["buckets"]):
                    if bucket["key"] == bucket_key:
                        b_id = index
                        pass
                    pass
                if b_id is None:
                    return None
                return getters[key](response_body["buckets"][b_id])
            return result_split

        if isinstance(is_axis, list):
            return {key + "_" + str(key2): split_factory(key2) for key2 in is_axis}
            pass
        elif is_axis:
            return {key: result_axis}
        else:
            return {key: result_plain}

    getters_new = {}
    for getter in getters:
        getters_new = {**getters_new, **getter_factory(getter)}

    if isinstance(is_axis, list):
        getter_updater = split_multi_bucket_getter_updater_factory(is_axis)
    elif is_axis:
        getter_updater = axis_multi_bucket_getter_updater
    else:
        getter_updater = plain_multi_bucket_getter_updater

    body = {"date_histogram" if date_histogram else "histogram": {"field": field, "interval": interval}}

    if is_axis and not isinstance(is_axis, list):
        return {"body": body, "getters": getters_new, "getter_updater": getter_updater, "sub_aggs": kwargs, "axis_maker": multi_bucket_axis_maker}
    else:
        return {"body": body, "getters": getters_new, "getter_updater": getter_updater, "sub_aggs": kwargs}


##############################################################################################
# VALUE AGGS                                                                                 #
##############################################################################################


def simple_value_agg(agg):
    def decorated_agg(*args, getter=None, **kwargs):
        getters = {}
        add_getter(getters, getter, "value")

        body = agg(*args, **kwargs)

        return {"body": body, "getters": getters}
    return decorated_agg


def agg_top_hits(size, sorting=None, fields=None, **kwargs):
    body = {
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
    return {"body": body, "getters": {}}


@simple_value_agg
def agg_cardinality(field, precision_threshold=40000, **kwargs):
    return {"cardinality": {"field": field, "precision_threshold": precision_threshold}}


@simple_value_agg
def agg_sum(field, script=False, **kwargs):
    return {"sum": {("script" if script else "field"): field}}


@simple_value_agg
def agg_avg(field, script=False, **kwargs):
    return {"avg": {("script" if script else "field"): field}}


@simple_value_agg
def agg_min(field, script=False, **kwargs):
    return {"min": {("script" if script else "field"): field}}


@simple_value_agg
def agg_max(field, script=False, **kwargs):
    return {"max": {("script" if script else "field"): field}}


@simple_value_agg
def agg_value_count(field, script=False, **kwargs):
    return {"value_count": {("script" if script else "field"): field}}


@simple_value_agg
def agg_avg_bucket(buckets_path, **kwargs):
    return {"avg_bucket": {"buckets_path": buckets_path}}


@simple_value_agg
def agg_derivative_bucket(buckets_path, **kwargs):
    return {"derivative_bucket": {"buckets_path": buckets_path}}


@simple_value_agg
def agg_max_bucket(buckets_path, **kwargs):
    return {"max_bucket": {"buckets_path": buckets_path}}


@simple_value_agg
def agg_min_bucket(buckets_path, **kwargs):
    return {"min_bucket": {"buckets_path": buckets_path}}


@simple_value_agg
def agg_sum_bucket(buckets_path, **kwargs):
    return {"sum_bucket": {"buckets_path": buckets_path}}


@simple_value_agg
def agg_stats_bucket(buckets_path, **kwargs):
    return {"stats_bucket": {"buckets_path": buckets_path}}


@simple_value_agg
def agg_extended_stats_bucket(buckets_path, **kwargs):
    return {"extended_stats_bucket": {"buckets_path": buckets_path}}


@simple_value_agg
def agg_cumulative_sum(buckets_path, **kwargs):
    return {"cumulative_sum": {"buckets_path": buckets_path}}


@simple_value_agg
def agg_bucket_selector(buckets_path, script, **kwargs):
    return {"bucket_selector": {"buckets_path": buckets_path, "script": script}}


@simple_value_agg
def agg_bucket_script(buckets_path, script, **kwargs):
    return {"bucket_script": {"buckets_path": buckets_path, "script": script}}


def agg_percentiles_bucket(buckets_path, percents=None, getter_key=None, getter_value=None, is_axis=True, **kwargs):
    getters = {}

    if getter_key is not None:
        if isinstance(is_axis, list):
            def split_factory(bucket_key):
                def result_split(response_body, *args, **kwargs2):
                    return bucket_key

                return result_split

            for key2 in is_axis:
                getters[getter_key + "_" + str(key2)] = split_factory(key2)
        elif is_axis:
            getters[getter_key] = lambda response_body, bucket_id, *args, **kwargs2: bucket_id
        else:
            getters[getter_key] = lambda response_body, *args, **kwargs2: list(response_body["values"].keys())

    if getter_value is not None:
        if isinstance(is_axis, list):
            def split_factory(bucket_key):
                def result_split(response_body, *args, **kwargs2):
                    return response_body["values"][bucket_key]

                return result_split

            for key2 in is_axis:
                getters[getter_key + "_" + str(key2)] = split_factory(key2)
        elif is_axis:
            getters[getter_value] = lambda response_body, bucket_id, *args, **kwargs2: response_body["values"][
                bucket_id]
        else:
            getters[getter_value] = lambda response_body, *args, **kwargs2: list(response_body["values"].values())
    body = {"percentiles_bucket": {"buckets_path": buckets_path, **({"percents": percents} if percents is not None else {})}}
    if is_axis and not isinstance(is_axis, list):
        return {"body": body, "getters": getters, "axis": percentile_axis_maker(None, None)}
    else:
        return {"body": body, "getters": getters}


def agg_extended_stats(field, script=False, sigma=3,
                       getter_count=None, getter_min=None, getter_max=None, getter_avg=None,
                       getter_sum=None, getter_sum_of_squares=None, getter_variance=None,
                       getter_deviation=None, getter_deviation_upper=None, getter_deviation_lower=None, **kwargs):
    getters = {}

    add_getter(getters, getter_count, "count")
    add_getter(getters, getter_min, "min")
    add_getter(getters, getter_max, "max")
    add_getter(getters, getter_avg, "avg")
    add_getter(getters, getter_sum, "sum")
    add_getter(getters, getter_sum_of_squares, "sum_of_squares")
    add_getter(getters, getter_variance, "variance")
    add_getter(getters, getter_deviation, "std_deviation")
    add_getter(getters, getter_deviation_upper, "upper", additional_level="std_deviation_bounds")
    add_getter(getters, getter_deviation_lower, "lower", additional_level="std_deviation_bounds")

    body = {"extended_stats": {("script" if script else "field"): field, "sigma": sigma}}

    return {"body": body, "getters": getters}


def agg_percentile(field, percents=None, getter_key=None, getter_value=None, is_axis=True, **kwargs):
    getters = {}

    if getter_key is not None:
        if isinstance(is_axis, list):
            def split_factory(bucket_key):
                def result_split(response_body, *args, **kwargs2):
                    return bucket_key
                return result_split
            for key2 in is_axis:
                getters[getter_key + "_" + str(key2)] = split_factory(key2)
        elif is_axis:
            getters[getter_key] = lambda response_body, bucket_id, *args, **kwargs2: bucket_id
        else:
            getters[getter_key] = lambda response_body, *args, **kwargs2: list(response_body["values"].keys())

    if getter_value is not None:
        if isinstance(is_axis, list):
            def split_factory(bucket_key):
                def result_split(response_body, *args, **kwargs2):
                    return response_body["values"][bucket_key]
                return result_split
            for key2 in is_axis:
                getters[getter_key + "_" + str(key2)] = split_factory(key2)
        elif is_axis:
            getters[getter_value] = lambda response_body, bucket_id, *args, **kwargs2: response_body["values"][bucket_id]
        else:
            getters[getter_value] = lambda response_body, *args, **kwargs2: list(response_body["values"].values())
    body = {"percentiles": {"field": field, **({"percents": percents} if percents is not None else {})}}
    if is_axis and not isinstance(is_axis, list):
        return {"body": body, "getters": getters, "axis": percentile_axis_maker(None, None)}
    else:
        return {"body": body, "getters": getters}


def agg_filters(filters, getter_key=None, getter_doc_count=None, is_axis=True, other_bucket_key=None, **kwargs):
    if isinstance(filters, list):
        return __agg_filters_anonymous(filters, getter_doc_count, other_bucket_key, is_axis, **kwargs)
    elif isinstance(filters, dict):
        return __agg_filters_named(filters, getter_key, getter_doc_count, other_bucket_key, is_axis, **kwargs)
    else:
        raise ValueError("Unsupported 'filters' type (use list or dict)")


@bucket_agg
def __agg_filters_anonymous(filters, getter_doc_count, other_bucket_key, is_axis, **kwargs):
    if isinstance(is_axis, list):
        raise ValueError("Anonymous filters aggregation can not be converted to plain")
    getters = {}
    add_getter(getters, getter_doc_count, "doc_count")

    def getter_factory(key):
        def result_plain(response_body, *args, **kwargs2):
            return [getters[key](bucket) for bucket in response_body["buckets"]]

        def result_axis(response_body, bucket_id, *args, **kwargs2):
            if len(response_body["buckets"]) > 0:
                return getters[key](response_body["buckets"][bucket_id])
            else:
                return None

        def split_factory(bucket_key):
            def result_split(response_body, *args, **kwargs2):
                b_id = None
                for index, bucket in enumerate(response_body["buckets"]):
                    if bucket["key"] == bucket_key:
                        b_id = index
                        pass
                    pass
                if b_id is None:
                    return None
                return getters[key](response_body["buckets"][b_id])

            return result_split

        if isinstance(is_axis, list):
            return {key + "_" + str(key2): split_factory(key2) for key2 in is_axis}
            pass
        elif is_axis:
            return {key: result_axis}
        else:
            return {key: result_plain}

    getters_new = {}
    for getter in getters:
        getters_new = {**getters_new, **getter_factory(getter)}

    if isinstance(is_axis, list):
        getter_updater = split_multi_bucket_getter_updater_factory(is_axis)
    elif is_axis:
        getter_updater = axis_multi_bucket_getter_updater
    else:
        getter_updater = plain_multi_bucket_getter_updater

    body = {
        "filters": {
            "filters": filters,
            **({} if other_bucket_key is None else {"other_bucket_key": other_bucket_key})
        }
    }

    if is_axis and not isinstance(is_axis, list):
        return {"body": body, "getters": getters_new, "getter_updater": getter_updater, "sub_aggs": kwargs,
                "axis_maker": multi_bucket_axis_maker}
    else:
        return {"body": body, "getters": getters_new, "getter_updater": getter_updater, "sub_aggs": kwargs}


@bucket_agg
def __agg_filters_named(filters, getter_key, getter_doc_count, other_bucket_key, is_axis, **kwargs):
    getters = {}
    add_getter(getters, getter_doc_count, "doc_count")

    def getter_factory(key):
        def result_plain(response_body, *args, **kwargs2):
            return [getters[key](bucket_value) for bucket_key, bucket_value in response_body["buckets"].items()]

        def result_axis(response_body, bucket_id, *args, **kwargs2):
            if len(response_body["buckets"]) > 0:
                return getters[key](response_body["buckets"][list(response_body["buckets"].keys())[bucket_id]])
            else:
                return None

        def split_factory(bucket_key):
            def result_split(response_body, *args, **kwargs2):
                return getters[key](response_body["buckets"][bucket_key])

            return result_split

        if isinstance(is_axis, list):
            return {key + "_" + str(key2): split_factory(key2) for key2 in is_axis}
            pass
        elif is_axis:
            return {key: result_axis}
        else:
            return {key: result_plain}

    getters_new = {}
    for getter in getters:
        getters_new = {**getters_new, **getter_factory(getter)}
    if getter_key is not None:
        if isinstance(is_axis, list):
            def split_factory(bucket_key):
                def result_split(response_body, *args, **kwargs2):
                    return bucket_key
                return result_split
            for key2 in is_axis:
                getters_new[getter_key + "_" + str(key2)] = split_factory(key2)
        elif is_axis:
            getters_new[getter_key] = lambda response_body, bucket_id, *args, **kwargs2: list(response_body["buckets"].keys())[bucket_id]
        else:
            getters_new[getter_key] = lambda response_body, *args, **kwargs2: list(response_body["buckets"].keys())

    if isinstance(is_axis, list):
        getter_updater = split_multi_bucket_getter_updater_factory(is_axis)
    elif is_axis:
        getter_updater = axis_multi_bucket_getter_updater
    else:
        getter_updater = plain_multi_bucket_getter_updater

    body = {
        "filters": {
            "filters": filters,
            **({} if other_bucket_key is None else {"other_bucket_key": other_bucket_key})
        }
    }

    if is_axis and not isinstance(is_axis, list):
        return {"body": body, "getters": getters_new, "getter_updater": getter_updater, "sub_aggs": kwargs,
                "axis_maker": multi_bucket_axis_maker}
    else:
        return {"body": body, "getters": getters_new, "getter_updater": getter_updater, "sub_aggs": kwargs}





