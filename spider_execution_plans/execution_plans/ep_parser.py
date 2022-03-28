from lxml.etree import _Element

from .ep_types import *

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def parse(ep: _Element) -> ExecutionPlan:
    stmt = ep.find(f".//{{{NS}}}StmtSimple")
    query = stmt.get("StatementText")
    return ExecutionPlan(query=query, relop=parse_relop(stmt.find(f".//{{{NS}}}RelOp")))


def is_compute_scalar(logical_op: str, physical_op: str) -> bool:
    return logical_op == physical_op == "Compute Scalar"


def is_stream_aggregate(logical_op: str, physical_op: str) -> bool:
    return logical_op == "Aggregate" and physical_op == "Stream Aggregate"


def is_index_scan(logical_op: str, physical_op: str) -> bool:
    return (
        logical_op == physical_op == "Clustered Index Scan"
        or logical_op == physical_op == "Clustered Index Seek"
        or logical_op == physical_op == "Index Scan"
        or logical_op == physical_op == "Index Seek"
        or logical_op == physical_op == "RID Lookup"
    )


def is_sort(logical_op: str, physical_op: str) -> bool:
    return logical_op in ("Sort", "Distinct Sort") and physical_op == "Sort"


def is_nested_loops(logical_op: str, physical_op: str) -> bool:
    return (
        logical_op in ("Inner Join", "Left Anti Semi Join", "Left Semi Join")
        and physical_op == "Nested Loops"
    )


def is_filter(logical_op: str, physical_op: str) -> bool:
    return logical_op == physical_op == "Filter"


def is_top_sort(logical_op: str, physical_op: str) -> bool:
    return logical_op == "TopN Sort" and physical_op == "Sort"


def is_top(logical_op: str, physical_op: str) -> bool:
    return logical_op == physical_op == "Top"


def is_merge(logical_op: str, physical_op: str) -> bool:
    return (
        logical_op
        in ("Union", "Inner Join", "Right Anti Semi Join", "Left Anti Semi Join")
        and physical_op == "Merge Join"
    )


def is_table_scan(logical_op: str, physical_op: str) -> bool:
    return logical_op == physical_op == "Table Scan"


def is_hash(logical_op: str, physical_op: str) -> bool:
    return (
        logical_op in ("Aggregate", "Inner Join", "Right Anti Semi Join")
        and physical_op == "Hash Match"
    )


def is_concat(logical_op: str, physical_op: str) -> bool:
    return logical_op == physical_op == "Concatenation"


def is_row_count_spool(logical_op: str, physical_op: str) -> bool:
    return logical_op == "Lazy Spool" and physical_op == "Row Count Spool"


def is_spool(logical_op: str, physical_op: str) -> bool:
    return logical_op == "Lazy Spool" and physical_op == "Table Spool"


def parse_relop(relop: _Element) -> RelOp:
    logical_op = relop.attrib["LogicalOp"]
    physical_op = relop.attrib["PhysicalOp"]
    output_list = [
        parse_column_reference(c)
        for c in relop.find(f"./{{{NS}}}OutputList").getchildren()
    ]

    if is_compute_scalar(logical_op, physical_op):
        operation = parse_compute_scalar(relop.find(f"./{{{NS}}}ComputeScalar"))
    elif is_stream_aggregate(logical_op, physical_op):
        operation = parse_stream_aggregate(relop.find(f"./{{{NS}}}StreamAggregate"))
    elif is_index_scan(logical_op, physical_op):
        operation = parse_index_scan(relop.find(f"./{{{NS}}}IndexScan"))
    elif is_sort(logical_op, physical_op):
        operation = parse_sort(relop.find(f"./{{{NS}}}Sort"))
    elif is_nested_loops(logical_op, physical_op):
        operation = parse_nested_loops(relop.find(f"./{{{NS}}}NestedLoops"))
    elif is_filter(logical_op, physical_op):
        operation = parse_filter(relop.find(f"./{{{NS}}}Filter"))
    elif is_top_sort(logical_op, physical_op):
        operation = parse_top_sort(relop.find(f"./{{{NS}}}TopSort"))
    elif is_top(logical_op, physical_op):
        operation = parse_top(relop.find(f"./{{{NS}}}Top"))
    elif is_merge(logical_op, physical_op):
        operation = parse_merge(relop.find(f"./{{{NS}}}Merge"))
    elif is_table_scan(logical_op, physical_op):
        operation = parse_table_scan(relop.find(f"./{{{NS}}}TableScan"))
    elif is_hash(logical_op, physical_op):
        operation = parse_hash(relop.find(f"./{{{NS}}}Hash"))
    elif is_concat(logical_op, physical_op):
        operation = parse_concat(relop.find(f"./{{{NS}}}Concat"))
    elif is_row_count_spool(logical_op, physical_op):
        operation = parse_row_count_spool(relop.find(f"./{{{NS}}}RowCountSpool"))
    elif is_spool(logical_op, physical_op):
        operation = parse_spool(relop.find(f"./{{{NS}}}Spool"))
    else:
        raise ValueError(
            f"The pair ({logical_op}, {physical_op}) are not mapped to any operation."
        )

    return RelOp(operation=operation, output_list=output_list)


def parse_scalar_operator(scalar_operator: _Element) -> ScalarOperator:
    operator = scalar_operator.getchildren()[0]
    tag = operator.tag
    if tag == f"{{{NS}}}Aggregate":
        return parse_aggregate(operator)
    if tag == f"{{{NS}}}Arithmetic":
        return parse_arithmetic(operator)
    if tag == f"{{{NS}}}Compare":
        return parse_compare(operator)
    if tag == f"{{{NS}}}Const":
        return parse_const(operator)
    if tag == f"{{{NS}}}Convert":
        return parse_convert(operator)
    if tag == f"{{{NS}}}IF":
        return parse_if(operator)
    if tag == f"{{{NS}}}Identifier":
        return parse_identifier(operator)
    if tag == f"{{{NS}}}Intrinsic":
        return parse_intrinsic(operator)
    if tag == f"{{{NS}}}Logical":
        return parse_logical(operator)
    raise ValueError(f"Unknown ScalarOperator: {tag}")


def parse_aggregate(aggregate: _Element) -> Aggregate:
    agg_type = aggregate.attrib["AggType"]
    distinct = aggregate.attrib["Distinct"]
    scalar_operators = [parse_scalar_operator(e) for e in aggregate.getchildren()]
    return Aggregate(
        agg_type=agg_type, distinct=distinct, scalar_operators=scalar_operators
    )


def parse_arithmetic(arithmetic: _Element) -> Arithmetic:
    operation = arithmetic.attrib["Operation"]
    scalar_operators = [parse_scalar_operator(e) for e in arithmetic.getchildren()]
    return Arithmetic(operation=operation, scalar_operators=scalar_operators)


def parse_compare(compare: _Element) -> Compare:
    compare_op = compare.attrib["CompareOp"]
    scalar_operators = [parse_scalar_operator(e) for e in compare.getchildren()]
    return Compare(compare_op=compare_op, scalar_operators=scalar_operators)


def parse_const(const: _Element) -> Const:
    value = const.attrib["ConstValue"]
    if value.startswith("("):
        if "e" in value or "." in value:
            value = float(value[1:-1])
        else:
            value = int(value[1:-1])
    return Const(const_value=value)


def parse_convert(convert: _Element) -> Convert:
    scalar_operator = parse_scalar_operator(convert.find(f"./{{{NS}}}ScalarOperator"))
    data_type = convert.attrib["DataType"]
    implicit = convert.attrib["Implicit"] == "1"
    length = convert.get("Length")
    precision = convert.get("Precision")
    scale = convert.get("Scale")
    return Convert(
        scalar_operator=scalar_operator,
        data_type=data_type,
        implicit=implicit,
        length=length,
        precision=precision,
        scale=scale,
    )


def parse_if(if_: _Element) -> If:
    condition = parse_scalar_operator(
        if_.find(f"./{{{NS}}}Condition/{{{NS}}}ScalarOperator")
    )
    then = parse_scalar_operator(if_.find(f"./{{{NS}}}Then/{{{NS}}}ScalarOperator"))
    alt = parse_scalar_operator(if_.find(f"./{{{NS}}}Else/{{{NS}}}ScalarOperator"))
    return If(condition=condition, then=then, alt=alt)


def parse_identifier(identifier: _Element) -> Identifier:
    column_reference = parse_column_reference(
        identifier.find(f"./{{{NS}}}ColumnReference")
    )
    return Identifier(column_reference=column_reference)


def parse_intrinsic(intrinsic: _Element) -> Intrinsic:
    function_name = intrinsic.attrib["FunctionName"]
    scalar_operators = [parse_scalar_operator(e) for e in intrinsic.getchildren()]
    return Intrinsic(function_name=function_name, scalar_operators=scalar_operators)


def parse_logical(logical: _Element) -> Logical:
    operation = logical.attrib["Operation"]
    scalar_operators = [parse_scalar_operator(e) for e in logical.getchildren()]
    return Logical(operation=operation, scalar_operators=scalar_operators)


def parse_defined_values(op: _Element) -> list[DefinedValue]:
    defined_values_path = f"./{{{NS}}}DefinedValues/{{{NS}}}DefinedValue"
    dvs: list[_Element] = op.findall(defined_values_path)
    result: list[DefinedValue] = []
    for dv in dvs:
        column_references = [
            parse_column_reference(c) for c in dv.findall(f"./{{{NS}}}ColumnReference")
        ]
        scalar_operator_element = dv.find(f"./{{{NS}}}ScalarOperator")
        if scalar_operator_element is not None:
            scalar_operator = parse_scalar_operator(scalar_operator_element)
            result.append(
                DefinedValue(
                    column_references=column_references, scalar_operator=scalar_operator
                )
            )
        else:
            result.append(DefinedValue(column_references=column_references))
    return result


def parse_compute_scalar(compute_scalar: _Element) -> ComputeScalar:
    relop = parse_relop(compute_scalar.find(f"./{{{NS}}}RelOp"))
    defined_values = parse_defined_values(compute_scalar)
    if (cs := compute_scalar.get("ComputeSequence")) is not None:
        compute_sequence = cs == "1"
        return ComputeScalar(
            compute_sequence=compute_sequence,
            relop=relop,
            defined_values=defined_values,
        )
    return ComputeScalar(relop=relop, defined_values=defined_values)


def parse_group_by(group_by: _Element) -> list[ColumnReference]:
    return [
        parse_column_reference(c)
        for c in group_by.findall(f"./{{{NS}}}ColumnReference")
    ]


def parse_column_reference(column_reference: _Element) -> ColumnReference:
    return ColumnReference(
        column=column_reference.attrib["Column"],
        schema=column_reference.get("Schema"),
        table=column_reference.get("Table"),
        alias=column_reference.get("Alias"),
    )


def parse_stream_aggregate(stream_aggregate: _Element) -> StreamAggregate:
    relop = parse_relop(stream_aggregate.find(f"./{{{NS}}}RelOp"))
    defined_values = parse_defined_values(stream_aggregate)
    if (group_by := stream_aggregate.find(f"./{{{NS}}}GroupBy")) is not None:
        return StreamAggregate(
            group_by=parse_group_by(group_by),
            relop=relop,
            defined_values=defined_values,
        )
    return StreamAggregate(relop=relop, defined_values=defined_values)


def parse_object(obj: _Element) -> Object:
    return Object(
        schema=obj.attrib["Schema"],
        table=obj.attrib["Table"],
        alias=obj.get("Alias"),
        index=obj.get("Index"),
    )


def parse_predicate(predicate: _Element) -> ScalarOperator:
    return parse_scalar_operator(predicate.find(f"./{{{NS}}}ScalarOperator"))


def parse_scan_range(scan_range: _Element) -> ScanRange:
    scan_type = scan_range.attrib["ScanType"]
    range_columns = [
        parse_column_reference(c)
        for c in scan_range.findall(f"./{{{NS}}}RangeColumns/{{{NS}}}ColumnReference")
    ]
    range_expressions = [
        parse_scalar_operator(so)
        for so in scan_range.findall(
            f"./{{{NS}}}RangeExpressions/{{{NS}}}ScalarOperator"
        )
    ]
    return ScanRange(
        scan_type=scan_type,
        range_columns=range_columns,
        range_expressions=range_expressions,
    )


def parse_seek_predicate(seek_predicate: _Element) -> SeekPredicate:
    if (prefix := seek_predicate.find(f"./{{{NS}}}Prefix")) is not None:
        prefix = parse_scan_range(prefix)
    if (start_range := seek_predicate.find(f"./{{{NS}}}StartRange")) is not None:
        start_range = parse_scan_range(start_range)
    if (end_range := seek_predicate.find(f"./{{{NS}}}EndRange")) is not None:
        end_range = parse_scan_range(end_range)
    return SeekPredicate(prefix=prefix, start_range=start_range, end_range=end_range)


def parse_index_scan(index_scan: _Element) -> IndexScan:
    obj = parse_object(index_scan.find(f"./{{{NS}}}Object"))
    defined_values = parse_defined_values(index_scan)
    ordered = index_scan.attrib["Ordered"] == "true"
    seek_predicate_path = (
        f"./{{{NS}}}SeekPredicates/{{{NS}}}SeekPredicateNew/{{{NS}}}SeekKeys"
    )
    if (seek_predicate := index_scan.find(seek_predicate_path)) is not None:
        seek_predicate = parse_seek_predicate(seek_predicate)
    predicates = [
        parse_predicate(e) for e in index_scan.findall(f"./{{{NS}}}Predicate")
    ]
    return IndexScan(
        ordered=ordered,
        obj=obj,
        seek_predicate=seek_predicate,
        predicates=predicates,
        defined_values=defined_values,
    )


def parse_order_by(order_by: _Element) -> OrderBy:
    order_by_column = order_by.find(f"./{{{NS}}}OrderByColumn")
    ascending = order_by_column.attrib["Ascending"] == "1"
    columns = [
        parse_column_reference(e)
        for e in order_by_column.findall(f"./{{{NS}}}ColumnReference")
    ]
    return OrderBy(ascending=ascending, columns=columns)


def parse_sort(sort: _Element) -> Sort:
    distinct = sort.attrib["Distinct"] == "1"
    order_by = parse_order_by(sort.find(f"./{{{NS}}}OrderBy"))
    relop = parse_relop(sort.find(f"./{{{NS}}}RelOp"))
    defined_values = parse_defined_values(sort)
    return Sort(
        distinct=distinct, order_by=order_by, relop=relop, defined_values=defined_values
    )


def parse_nested_loops(nested_loops: _Element) -> NestedLoops:
    left, right = [parse_relop(e) for e in nested_loops.findall(f"./{{{NS}}}RelOp")]
    defined_values = parse_defined_values(nested_loops)
    if (p := nested_loops.find(f"./{{{NS}}}Predicate")) is not None:
        return NestedLoops(
            left=left,
            right=right,
            predicate=parse_predicate(p),
            defined_values=defined_values,
        )
    return NestedLoops(left=left, right=right, defined_values=defined_values)


def parse_filter(filter_: _Element) -> Filter:
    startup_expression = filter_.attrib["StartupExpression"] == "1"
    relop = parse_relop(filter_.find(f"./{{{NS}}}RelOp"))
    predicate = parse_predicate(filter_.find(f"./{{{NS}}}Predicate"))
    defined_values = parse_defined_values(filter_)
    return Filter(
        startup_expression=startup_expression,
        relop=relop,
        predicate=predicate,
        defined_values=defined_values,
    )


def parse_top_sort(top_sort: _Element) -> TopSort:
    rows = int(top_sort.attrib["Rows"])
    distinct = top_sort.attrib["Distinct"] == "1"
    order_by = parse_order_by(top_sort.find(f"./{{{NS}}}OrderBy"))
    relop = parse_relop(top_sort.find(f"./{{{NS}}}RelOp"))
    defined_values = parse_defined_values(top_sort)
    return TopSort(
        distinct=distinct,
        order_by=order_by,
        relop=relop,
        rows=rows,
        defined_values=defined_values,
    )


def parse_top(top: _Element) -> Top:
    top_expression = parse_scalar_operator(
        top.find(f"./{{{NS}}}TopExpression/{{{NS}}}ScalarOperator")
    )
    relop = parse_relop(top.find(f"./{{{NS}}}RelOp"))
    defined_values = parse_defined_values(top)
    return Top(
        top_expression=top_expression, relop=relop, defined_values=defined_values
    )


def parse_merge(merge: _Element) -> Merge:
    left, right = [parse_relop(e) for e in merge.findall(f"./{{{NS}}}RelOp")]
    join_tags = [f"{{{NS}}}InnerSideJoinColumns", f"{{{NS}}}OuterSideJoinColumns"]
    defined_values = parse_defined_values(merge)
    if all([merge.find(t) is not None for t in join_tags]):
        on_left = parse_column_reference(
            merge.find(join_tags[0]).find(f"{{{NS}}}ColumnReference")
        )
        on_right = parse_column_reference(
            merge.find(join_tags[1]).find(f"{{{NS}}}ColumnReference")
        )
        return Merge(
            left=left,
            right=right,
            on_left=on_left,
            on_right=on_right,
            defined_values=defined_values,
        )
    return Merge(left=left, right=right, defined_values=defined_values)


def parse_table_scan(table_scan: _Element) -> TableScan:
    ordered = table_scan.attrib["Ordered"] == "1"
    obj = parse_object(table_scan.find(f"./{{{NS}}}Object"))
    defined_values = parse_defined_values(table_scan)
    if (p := table_scan.find(f"./{{{NS}}}Predicate")) is not None:
        return TableScan(
            ordered=ordered,
            obj=obj,
            predicate=parse_predicate(p),
            defined_values=defined_values,
        )
    return TableScan(ordered=ordered, obj=obj, defined_values=defined_values)


def parse_hash(hash_: _Element) -> Hash:
    relops = [parse_relop(e) for e in hash_.findall(f"./{{{NS}}}RelOp")]
    defined_values = parse_defined_values(hash_)
    return Hash(relops=relops, defined_values=defined_values)


def parse_concat(concat: _Element) -> Concat:
    relops = [parse_relop(e) for e in concat.findall(f"./{{{NS}}}RelOp")]
    defined_values = parse_defined_values(concat)
    return Concat(relops=relops, defined_values=defined_values)


def parse_row_count_spool(row_count_spool: _Element) -> RowCountSpool:
    relop = parse_relop(row_count_spool.find(f"./{{{NS}}}RelOp"))
    defined_values = parse_defined_values(row_count_spool)
    return RowCountSpool(relop=relop, defined_values=defined_values)


def parse_spool(spool: _Element) -> Spool:
    relop = parse_relop(spool.find(f"./{{{NS}}}RelOp"))
    defined_values = parse_defined_values(spool)
    return Spool(relop=relop, defined_values=defined_values)
