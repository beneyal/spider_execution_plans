from typing import Callable

import graphviz

from .ep_types import *

FONT = "JetBrainsMono NF"


def get_indexer(label: str) -> Callable[[], str]:
    i = 0

    def f():
        nonlocal i
        i += 1
        return f"{label}_{i}"

    return f


NODE_NAMES = [
    "ComputeScalar",
    "Concat",
    "Filter",
    "Hash",
    "IndexScan",
    "Merge",
    "NestedLoops",
    "RowCountSpool",
    "Sort",
    "Spool",
    "StreamAggregate",
    "TableScan",
    "Top",
    "TopSort",
    "Aggregate",
    "Arithmetic",
    "Compare",
    "Const",
    "Convert",
    "If",
    "Identifier",
    "Intrinsic",
    "Logical",
    "Array"
]

node2indexer: dict[str, Callable[[], str]] = {node_name: get_indexer(node_name) for node_name in NODE_NAMES}


def draw_execution_plan(parsed_ep: ExecutionPlan,
                        graph_name: str = "ExecutionPlan",
                        save_dir: Optional[str] = None,
                        format_: Optional[str] = None) -> None:
    dot = graphviz.Digraph(name=graph_name,
                           format=format_,
                           graph_attr={
                               "rankdir": "RL",
                               "labelloc": "t",
                               "label": parsed_ep.query,
                               "fontname": FONT
                           },
                           node_attr={"shape": "record", "fontname": FONT},
                           edge_attr={"fontname": FONT})
    root = "SELECT"
    dot.node(root)
    draw_relop(parsed_ep.relop, root, dot)
    if save_dir:
        dot.render(directory=save_dir)
    else:
        dot.render(directory="tmp", view=True)


def draw_relop(relop: RelOp, prev_node: str, dot: graphviz.Digraph) -> None:
    if isinstance(relop.operation, ComputeScalar):
        t = draw_compute_scalar(relop.operation, dot)
    elif isinstance(relop.operation, StreamAggregate):
        t = draw_stream_aggregate(relop.operation, dot)
    elif isinstance(relop.operation, IndexScan):
        t = draw_index_scan(relop.operation, dot)
    elif isinstance(relop.operation, Sort):
        t = draw_sort(relop.operation, dot)
    elif isinstance(relop.operation, NestedLoops):
        t = draw_nested_loops(relop.operation, dot)
    elif isinstance(relop.operation, Filter):
        t = draw_filter(relop.operation, dot)
    elif isinstance(relop.operation, TopSort):
        t = draw_top_sort(relop.operation, dot)
    elif isinstance(relop.operation, Top):
        t = draw_top(relop.operation, dot)
    elif isinstance(relop.operation, Merge):
        t = draw_merge(relop.operation, dot)
    elif isinstance(relop.operation, TableScan):
        t = draw_table_scan(relop.operation, dot)
    elif isinstance(relop.operation, Hash):
        t = draw_hash(relop.operation, dot)
    elif isinstance(relop.operation, Concat):
        t = draw_concat(relop.operation, dot)
    elif isinstance(relop.operation, RowCountSpool):
        t = draw_row_count_spool(relop.operation, dot)
    elif isinstance(relop.operation, Spool):
        t = draw_spool(relop.operation, dot)
    else:
        raise ValueError(f"{type(relop.operation)} does not exist.")

    dot.edge(t, prev_node, label=", ".join(x.column for x in relop.output_list))


def draw_compute_scalar(cs: ComputeScalar, dot: graphviz.Digraph):
    node = node2indexer[type(cs).__name__]()
    dot.node(node, label=str(cs))
    draw_relop(cs.relop, node, dot)
    return node


def draw_stream_aggregate(sa: StreamAggregate, dot: graphviz.Digraph):
    node = node2indexer[type(sa).__name__]()
    dot.node(node, label=str(sa))
    draw_relop(sa.relop, node, dot)
    return node


def draw_index_scan(isc: IndexScan, dot: graphviz.Digraph):
    node = node2indexer[type(isc).__name__]()
    dot.node(node, label=str(isc))
    return node


def draw_sort(s: Sort, dot: graphviz.Digraph):
    node = node2indexer[type(s).__name__]()
    dot.node(node, label=str(s))
    draw_relop(s.relop, node, dot)
    return node


def draw_nested_loops(nl: NestedLoops, dot: graphviz.Digraph):
    node = node2indexer[type(nl).__name__]()
    dot.node(node, label=str(nl))
    draw_relop(nl.left, node, dot)
    draw_relop(nl.right, node, dot)
    return node


def draw_filter(f: Filter, dot: graphviz.Digraph):
    node = node2indexer[type(f).__name__]()
    dot.node(node, label=str(f))
    draw_relop(f.relop, node, dot)
    return node


def draw_top_sort(ts: TopSort, dot: graphviz.Digraph):
    node = node2indexer[type(ts).__name__]()
    dot.node(node, label=str(ts))
    draw_relop(ts.relop, node, dot)
    return node


def draw_top(t: Top, dot: graphviz.Digraph):
    node = node2indexer[type(t).__name__]()
    dot.node(node, label=str(t))
    draw_relop(t.relop, node, dot)
    return node


def draw_merge(m: Merge, dot: graphviz.Digraph):
    node = node2indexer[type(m).__name__]()
    dot.node(node, label=str(m))
    draw_relop(m.left, node, dot)
    draw_relop(m.right, node, dot)
    return node


def draw_table_scan(ts: TableScan, dot: graphviz.Digraph):
    node = node2indexer[type(ts).__name__]()
    dot.node(node, label=str(ts))
    return node


def draw_hash(h: Hash, dot: graphviz.Digraph):
    node = node2indexer[type(h).__name__]()
    dot.node(node, label=str(h))
    for relop in h.relops:
        draw_relop(relop, node, dot)
    return node


def draw_concat(c: Concat, dot: graphviz.Digraph):
    node = node2indexer[type(c).__name__]()
    dot.node(node, label=str(c))
    for relop in c.relops:
        draw_relop(relop, node, dot)
    return node


def draw_row_count_spool(rcs: RowCountSpool, dot: graphviz.Digraph):
    node = node2indexer[type(rcs).__name__]()
    dot.node(node, label=str(rcs))
    draw_relop(rcs.relop, node, dot)
    return node


def draw_spool(s: Spool, dot: graphviz.Digraph):
    node = node2indexer[type(s).__name__]()
    dot.node(node, label=str(s))
    draw_relop(s.relop, node, dot)
    return node
