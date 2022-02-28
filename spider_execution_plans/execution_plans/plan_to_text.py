from .ep_types import *


def plan_to_text(ep: ExecutionPlan) -> str:
    """Convert an Execution Plan to a set of textual instructions.

    Parameters
    ----------
    ep
        The execution plan to be converted

    Returns
    -------
    plan
        Text consisting of numbered instructions according to the given plan.
    """
    instructions = relop_to_text(ep.relop)
    return "\n".join(instructions)


def relop_to_text(relop: RelOp) -> list[str]:
    """Convert a RelOp (the abstract type of the execution plan nodes) to text.

    Parameters
    ----------
    relop
        The RelOp to convert.

    Returns
    -------
    instructions
        A list of instructions based on the specific RelOp.
    """
    if isinstance(relop.operation, ComputeScalar):
        return compute_scalar_to_text(relop.operation)
    elif isinstance(relop.operation, StreamAggregate):
        return stream_aggregate_to_text(relop.operation)
    elif isinstance(relop.operation, IndexScan):
        return index_scan_to_text(relop.operation)
    elif isinstance(relop.operation, Sort):
        return sort_to_text(relop.operation)
    elif isinstance(relop.operation, NestedLoops):
        return nested_loops_to_text(relop.operation)
    elif isinstance(relop.operation, Filter):
        return filter_to_text(relop.operation)
    elif isinstance(relop.operation, TopSort):
        return top_sort_to_text(relop.operation)
    elif isinstance(relop.operation, Top):
        return top_to_text(relop.operation)
    elif isinstance(relop.operation, Merge):
        return merge_to_text(relop.operation)
    elif isinstance(relop.operation, TableScan):
        return table_scan_to_text(relop.operation)
    elif isinstance(relop.operation, Hash):
        return hash_to_text(relop.operation)
    elif isinstance(relop.operation, Concat):
        return concat_to_text(relop.operation)
    elif isinstance(relop.operation, RowCountSpool):
        return row_count_spool_to_text(relop.operation)
    elif isinstance(relop.operation, Spool):
        return spool_to_text(relop.operation)
    else:
        raise ValueError(f"{type(relop.operation)} does not exist.")


def compute_scalar_to_text(x: ComputeScalar) -> list[str]:
    """

    Parameters
    ----------
    x

    Returns
    -------

    """
    instructions = relop_to_text(x.relop)
    compute_scalar_instructions = [
        f"Compute {dv.scalar_operator} and store it as {dv.column_references[0]}."
        for dv in x.defined_values
    ]
    return instructions + compute_scalar_instructions


def stream_aggregate_to_text(x: StreamAggregate) -> list[str]:
    """

    Parameters
    ----------
    x

    Returns
    -------

    """
    pass


def index_scan_to_text(x: IndexScan) -> list[str]:
    """Convert an IndexScan node to text.

    Parameters
    ----------
    x
        An IndexScan node

    Returns
    -------
    instructions
        A list consisting of a single instruction: "Scan <table> using <method> [in order] [by checking <predicate>+]"
    """
    ordered = " in order" if x.ordered else ""
    predicates = (
        " by checking " + ", ".join(str(p) for p in x.predicates).replace("\\", "")
        if x.predicates
        else ""
    )
    return [f"Scan {x.obj}{ordered}{predicates}"]


def sort_to_text(x: Sort) -> list[str]:
    """Convert a Sort node to text.

    Parameters
    ----------
    x
        A Sort node

    Returns
    -------
    instructions
        A list consisting of [RelOp realization, "Sort distinct values col1, col2, ... in ascending order"]
    """
    instructions = relop_to_text(x.relop)
    sort_columns = [f"{cr.table}.{cr.column}" for cr in x.order_by.columns]
    order = "ascending" if x.order_by.ascending else "descending"
    distinct = "distinct" if x.distinct else ""
    instruction = f"Sort {distinct} values {', '.join(sort_columns)} in {order} order."
    return instructions + [instruction]


def nested_loops_to_text(x: NestedLoops) -> list[str]:
    """

    Parameters
    ----------
    x

    Returns
    -------

    """
    pass


def filter_to_text(x: Filter) -> list[str]:
    """

    Parameters
    ----------
    x

    Returns
    -------

    """
    pass


def top_sort_to_text(x: TopSort) -> list[str]:
    """

    Parameters
    ----------
    x

    Returns
    -------

    """
    pass


def top_to_text(x: Top) -> list[str]:
    """

    Parameters
    ----------
    x

    Returns
    -------

    """
    pass


def merge_to_text(x: Merge) -> list[str]:
    """

    Parameters
    ----------
    x

    Returns
    -------

    """
    pass


def table_scan_to_text(x: TableScan) -> list[str]:
    """Convert a TableScan node to text.

    Parameters
    ----------
    x
        A TableScan node

    Returns
    -------
    instructions
        A list consisting of a single instruction: "Scan <table> using <method> [in order] [by checking <predicate>+]"
    """
    ordered = " in order" if x.ordered else ""
    predicates = (
        " by checking " + ", ".join(str(p) for p in x.predicates).replace("\\", "")
        if x.predicates
        else ""
    )
    return [f"Scan {x.obj}{ordered}{predicates}"]


def hash_to_text(x: Hash) -> list[str]:
    """Convert a Hash node to text.

    Parameters
    ----------
    x
        A Hash node

    Returns
    -------
    instructions
        A list consisting of the Hash node's RelOp instructions, appended with "".
    """
    instructions = [ins for relop in x.relops for ins in relop_to_text(relop)]
    # TODO


def concat_to_text(x: Concat) -> list[str]:
    """Convert a Concat node to text.

    Parameters
    ----------
    x
        A Concat node

    Returns
    -------
    instructions
        A list consisting of the instructions of the N RelOps connected to the Concat node,
        appended with "Concatenate the results of the previous N outputs".
    """
    instructions = [ins for relop in x.relops for ins in relop_to_text(relop)]
    instruction = f"Concatenate the outputs of the previous {len(x.relops)} nodes."
    return instructions + [instruction]


def row_count_spool_to_text(x: RowCountSpool) -> list[str]:
    """Convert a RowCountSpool node to text.

    Parameters
    ----------
    x
        A RowCountSpool node

    Returns
    -------
    instructions
        A list consisting of the node's RelOp instructions, appended with "Count the number of rows in the result".
    """
    instructions = relop_to_text(x.relop)
    instruction = "Count the number of rows in the result."
    return instructions + [instruction]


def spool_to_text(x: Spool) -> list[str]:
    """

    Parameters
    ----------
    x

    Returns
    -------

    """
    pass
