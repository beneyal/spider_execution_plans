from dataclasses import dataclass, field
from typing import Literal, Optional, Union


@dataclass(frozen=True)
class ColumnReference:
    column: str
    schema: Optional[str] = None
    table: Optional[str] = None
    alias: Optional[str] = None

    def __str__(self):
        s = []
        if self.table:
            s.append(self.table)
        s.append(self.column)
        result = ".".join(s)
        # if self.alias:
        #     result += f" AS {self.alias}.{self.column}"
        return result


@dataclass(frozen=True)
class OrderBy:
    ascending: bool
    columns: list[ColumnReference] = field(default_factory=list)

    def __str__(self):
        return "Order By: {} ({})".format(
            "\n".join(map(str, self.columns)), "ASC" if self.ascending else "DESC"
        )


@dataclass(frozen=True)
class Aggregate:
    agg_type: str
    distinct: bool
    scalar_operators: list["ScalarOperator"] = field(default_factory=list)

    def __str__(self):
        if self.scalar_operators:
            return (
                f"{self.agg_type}"
                f"({'DISTINCT ' if self.distinct else ''}"
                f"{', '.join(map(str, self.scalar_operators))})"
            )
        else:
            return self.agg_type


ARITHMETIC_OPERATION = Literal["ADD", "DIV", "SUB"]
arith2sign = {"ADD": "+", "DIV": "/", "SUB": "-"}


@dataclass(frozen=True)
class Arithmetic:
    operation: ARITHMETIC_OPERATION
    scalar_operators: list["ScalarOperator"] = field(default_factory=list)

    def __str__(self):
        assert len(self.scalar_operators) == 2
        return f"{self.scalar_operators[0]} {arith2sign[self.operation]} {self.scalar_operators[1]}"


COMPARE_OP = Literal["EQ", "GE", "GT", "IS", "LE", "LT", "NE"]
comp2sign = {
    "EQ": "=",
    "GE": "\>=",
    "GT": "\>",
    "IS": "IS",
    "LE": "\<=",
    "LT": "\<",
    "NE": "\<\>",
}


@dataclass(frozen=True)
class Compare:
    compare_op: COMPARE_OP
    scalar_operators: list["ScalarOperator"] = field(default_factory=list)

    def __str__(self):
        assert len(self.scalar_operators) == 2
        return f"{self.scalar_operators[0]} {comp2sign[self.compare_op]} {self.scalar_operators[1]}"


@dataclass(frozen=True)
class Const:
    const_value: str

    def __str__(self):
        return str(self.const_value)


@dataclass(frozen=True)
class Convert:
    scalar_operator: "ScalarOperator"
    data_type: str
    implicit: bool
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None

    def __str__(self):
        return f"Convert({self.scalar_operator}, {self.data_type})"


@dataclass(frozen=True)
class If:
    condition: "ScalarOperator"
    then: "ScalarOperator"
    alt: "ScalarOperator"

    def __str__(self):
        return f"IF {self.condition} {self.then}; ELSE {self.alt};"


@dataclass(frozen=True)
class Identifier:
    column_reference: ColumnReference

    def __str__(self):
        return str(self.column_reference)


@dataclass(frozen=True)
class Intrinsic:
    function_name: str  # in our dataset, "function_name" is always "like"
    scalar_operators: list["ScalarOperator"] = field(default_factory=list)

    def __str__(self):
        left, right = self.scalar_operators
        return f"{left} {self.function_name} {right}"


LOGICAL_OPERATION = Literal["AND", "IS NULL", "OR"]


@dataclass(frozen=True)
class Logical:
    operation: LOGICAL_OPERATION
    scalar_operators: list["ScalarOperator"] = field(default_factory=list)

    def __str__(self):
        return f" {self.operation} ".join(map(str, self.scalar_operators))


ScalarOperator = Union[
    Aggregate,
    Arithmetic,
    Compare,
    Const,
    Convert,
    If,
    Identifier,
    Intrinsic,
    Logical,
]


@dataclass(frozen=True)
class DefinedValue:
    column_references: list[ColumnReference] = field(default_factory=list)
    scalar_operator: Optional[ScalarOperator] = None

    def __str__(self):
        if self.scalar_operator:
            return f"{self.column_references[0]} \u2190 {self.scalar_operator}"
        if len(self.column_references) == 3:  # UNION case
            u, c1, c2 = self.column_references
            return f"{u} \u2190 {c1} \u222A {c2}"
        return ", ".join(map(str, self.column_references))


@dataclass(frozen=True)
class ComputeScalar:
    relop: "RelOp"
    compute_sequence: Optional[bool] = None
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        if self.defined_values:
            return r"Compute Scalar|Defined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return "Compute Scalar"


@dataclass(frozen=True)
class Concat:
    relops: list["RelOp"] = field(default_factory=list)
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        if self.defined_values:
            return r"Concat|Defined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return "Concat"


@dataclass(frozen=True)
class Filter:
    startup_expression: bool
    relop: "RelOp"
    predicate: ScalarOperator
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        result = rf"Filter|Predicate:\n{self.predicate}"
        if self.defined_values:
            result += r"\n\nDefined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return result


@dataclass(frozen=True)
class Hash:
    relops: list["RelOp"] = field(default_factory=list)
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        if self.defined_values:
            return r"Hash|Defined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return "Hash"


@dataclass(frozen=True)
class Object:
    schema: str
    table: str
    alias: Optional[str] = None
    index: Optional[str] = None

    def __str__(self):
        if self.index:
            index_type = self.index[1:-1].split("__")[0]
        else:
            index_type = ""
        # alias = self.alias if self.alias else ""
        result = [f"{self.table} table"]
        # if alias:
        #     result.append(f"as {alias}")
        if index_type not in ("PK", "UQ"):
            result.append(f"using user index")
        return ", ".join(result)


@dataclass(frozen=True)
class ScanRange:
    scan_type: COMPARE_OP
    range_columns: list[ColumnReference]
    range_expressions: list[ScalarOperator]

    def __str__(self):
        # assert len(self.range_columns) == len(self.range_expressions)
        # assert len(self.range_columns) > 0
        # meaningful_comparisons = []
        # for range_col, range_expr in zip(self.range_columns, self.range_expressions):
        #     if isinstance(range_expr, Identifier):
        #         cr = range_expr.column_reference
        #         if range_col.table != cr.table or range_col.column != cr.column:
        #             meaningful_comparisons.append((range_col, cr))
        #     elif isinstance(range_expr, (Const, Convert)):
        #         meaningful_comparisons.append((range_col, range_expr))
        #     else:
        #         raise ValueError(f"RangeExpression {range_expr} of type "
        #                          f"{range_expr.__class__.__name__} not an Identifier")
        # assert len(meaningful_comparisons) == 1, meaningful_comparisons
        # c1, c2 = meaningful_comparisons[0]
        # return f"{c1} {comp2sign[self.scan_type]} {c2}"
        return "TODO"


@dataclass(frozen=True)
class SeekPredicate:
    prefix: Optional[ScanRange]
    start_range: Optional[ScanRange]
    end_range: Optional[ScanRange]

    def __str__(self):
        if self.prefix:
            return f"JOIN on {self.prefix}"
        if self.start_range and not self.end_range:
            return "TODO StartRange"
        return "TODO Start + End"


@dataclass(frozen=True)
class IndexScan:
    ordered: bool
    obj: Object
    seek_predicate: Optional[SeekPredicate] = None
    predicates: list[ScalarOperator] = field(default_factory=list)
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        result = rf"Index Scan|Scan Object: {self.obj}\nOrdered? {self.ordered}"
        if self.seek_predicate:
            result += r"\n\nSeek Predicate:\n{}".format(self.seek_predicate)
        if self.predicates:
            result += r"\n\nPredicates:\n{}".format(
                r"\n".join(list(map(str, self.predicates)))
            )
        if self.defined_values:
            result += r"\n\nDefined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return result


@dataclass(frozen=True)
class Merge:
    left: "RelOp"
    right: "RelOp"
    on_left: Optional[ColumnReference] = None
    on_right: Optional[ColumnReference] = None
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        result = rf"Merge|LHS: {self.on_left}\nRHS: {self.on_right}"
        if self.defined_values:
            result += r"\n\nDefined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return result


@dataclass(frozen=True)
class NestedLoops:
    left: "RelOp"
    right: "RelOp"
    predicate: Optional[ScalarOperator] = None
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        result = rf"Nested Loops"
        if self.predicate or self.defined_values:
            result += "|"
        if self.predicate:
            result += rf"Predicate: {self.predicate}\n"
        if self.defined_values:
            result += r"\nDefined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return result


@dataclass(frozen=True)
class RowCountSpool:
    relop: "RelOp"
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        if self.defined_values:
            return r"Row Count Spool|Defined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return "Row Count Spool"


@dataclass(frozen=True)
class Sort:
    distinct: bool
    order_by: OrderBy
    relop: "RelOp"
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        return rf"Sort|Distinct: {self.distinct}\n{self.order_by}"


@dataclass(frozen=True)
class Spool:
    relop: "RelOp"
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        if self.defined_values:
            return r"Spool|Defined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return "Spool"


@dataclass(frozen=True)
class StreamAggregate:
    relop: "RelOp"
    group_by: list[ColumnReference] = field(default_factory=list)
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        result = "Stream Aggregate|"
        if self.group_by:
            result += r"Grouped by: {}".format(r"\n".join(map(str, self.group_by)))
        if self.defined_values:
            if result.endswith("|"):
                result += r"Defined Values:\n{}".format(
                    r"\n".join(map(str, self.defined_values))
                )
            else:
                result += r"\nDefined Values:\n{}".format(
                    r"\n".join(map(str, self.defined_values))
                )
        return result


@dataclass(frozen=True)
class TableScan:
    ordered: bool
    obj: Object
    predicate: Optional[ScalarOperator] = None
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        result = rf"Table Scan|Scan Object: {self.obj}\nOrdered? {self.ordered}"
        if self.predicate:
            result += rf"\n\nPredicate:\n{self.predicate}\n"
        if self.defined_values:
            result += r"\n\nDefined Values:\n{}".format(
                r"\n".join(map(str, self.defined_values))
            )
        return result


@dataclass(frozen=True)
class Top:
    top_expression: ScalarOperator
    relop: "RelOp"
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        if self.defined_values:
            return r"Top|Expression: {}\n\nDefined Values:\n{}".format(
                self.top_expression, "\n".join(map(str, self.defined_values))
            )
        return f"Top|Expression: {self.top_expression}"


@dataclass(frozen=True)
class TopSort:
    rows: int
    distinct: bool
    order_by: OrderBy
    relop: "RelOp"
    defined_values: list[DefinedValue] = field(default_factory=list)

    def __str__(self):
        if self.defined_values:
            return (
                rf"Top Sort|Distinct: {self.distinct}\n{self.order_by}"
                + r"\n\nDefined Values:\n{}".format(
                    r"\n".join(map(str, self.defined_values))
                )
            )
        return rf"Top Sort|Distinct: {self.distinct}\n{self.order_by}"


RelOpType = Union[
    ComputeScalar,
    Concat,
    Filter,
    Hash,
    IndexScan,
    Merge,
    NestedLoops,
    RowCountSpool,
    Sort,
    Spool,
    StreamAggregate,
    TableScan,
    Top,
    TopSort,
]


@dataclass(frozen=True)
class RelOp:
    operation: RelOpType
    output_list: list[ColumnReference]
    defined_values: list[DefinedValue] = field(default_factory=list)


@dataclass(frozen=True)
class ExecutionPlan:
    query: str
    relop: RelOp
