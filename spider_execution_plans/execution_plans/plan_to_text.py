from typing import Any

from .ep_types import *

Environment = dict[str, Any]
IGNORED_NODES = (NestedLoops, ComputeScalar, Spool)


def apply_env(v: str, env: Environment):
    while v.startswith("Expr"):
        v = env[v]
    return v


@dataclass(frozen=True)
class Instruction:
    """Represents a single execution plan node's text"""

    text: str
    idx: int


def plan_to_text(ep: ExecutionPlan) -> str:
    env = {"idx": 1}
    instructions = relop_to_text(ep.relop, env=env)
    print(f"Env: {env}")
    return "\n".join([f"{ins.idx}. {ins.text}" for ins in instructions])


def relop_to_text(relop: RelOp, env: Environment) -> list[Instruction]:
    if isinstance(relop.operation, ComputeScalar):
        instructions = compute_scalar_to_text(relop.operation, env)
    elif isinstance(relop.operation, StreamAggregate):
        instructions = stream_aggregate_to_text(relop.operation, env)
    elif isinstance(relop.operation, IndexScan):
        instructions = index_scan_to_text(relop.operation, env)
    elif isinstance(relop.operation, Sort):
        instructions = sort_to_text(relop.operation, env)
    elif isinstance(relop.operation, NestedLoops):
        instructions = nested_loops_to_text(relop.operation, env)
    elif isinstance(relop.operation, Filter):
        instructions = filter_to_text(relop.operation, env)
    elif isinstance(relop.operation, TopSort):
        assert len(relop.output_list) > 0
        env["top_sort"] = relop.output_list
        instructions = top_sort_to_text(relop.operation, env)
    elif isinstance(relop.operation, Top):
        assert len(relop.output_list) > 0
        env["top"] = relop.output_list
        instructions = top_to_text(relop.operation, env)
    elif isinstance(relop.operation, Merge):
        instructions = merge_to_text(relop.operation, env)
    elif isinstance(relop.operation, TableScan):
        instructions = table_scan_to_text(relop.operation, env)
    elif isinstance(relop.operation, Hash):
        instructions = hash_to_text(relop.operation, env)
    elif isinstance(relop.operation, Concat):
        instructions = concat_to_text(relop.operation, env)
    elif isinstance(relop.operation, RowCountSpool):
        instructions = row_count_spool_to_text(relop.operation, env)
    elif isinstance(relop.operation, Spool):
        instructions = spool_to_text(relop.operation, env)
    else:
        raise ValueError(f"{type(relop.operation).__class__.__name__} does not exist.")

    if relop.output_list and not isinstance(relop.operation, IGNORED_NODES):
        last_ins = instructions[-1]
        text = last_ins.text
        idx = last_ins.idx
        text = f"{text[:-1]}, returning {', '.join(map(lambda x: apply_env(str(x), env), relop.output_list))}."
        instructions = instructions[:-1] + [Instruction(text=text, idx=idx)]

    return instructions


def compute_scalar_to_text(x: ComputeScalar, env: Environment) -> list[Instruction]:
    for dv in x.defined_values:
        assert len(dv.column_references) == 1

        env[dv.column_references[0].column] = scalar_operator_to_text(
            dv.scalar_operator, env
        )
    return relop_to_text(x.relop, env)


def stream_aggregate_to_text(x: StreamAggregate, env: Environment) -> list[Instruction]:
    instructions = relop_to_text(x.relop, env)
    stream_agg_instructions = []
    for dv in x.defined_values:
        scalar_operator = dv.scalar_operator
        column = dv.column_references[0]
        env[str(column)] = scalar_operator_to_text(scalar_operator, env)
        column = env[str(column)]
        agg_type = scalar_operator.agg_type

        assert len(x.group_by) == 1

        if agg_type == "ANY":
            # Explicitly ignore ANY aggregations
            pass
        elif agg_type == "countstar":
            stream_agg_instructions.append(f"count the number of rows")
        elif agg_type == "COUNT_BIG":
            stream_agg_instructions.append(
                f"count the number of non-null rows in {column}"
            )
        elif agg_type == "MAX":
            stream_agg_instructions.append(f"take the maximum value in {column}")
        elif agg_type == "MIN":
            stream_agg_instructions.append(f"take the minimum value in {column}")
        elif agg_type == "SUM":
            stream_agg_instructions.append(f"sum the rows of {column}")
        else:
            raise ValueError(
                f"Aggregate function {agg_type} not seen in train or dev sets"
            )
    if len(stream_agg_instructions) > 1:
        text = (
            f"Group rows by {x.group_by[0]} and for each group, "
            f"{', '.join(stream_agg_instructions[:-1])}, "
            f"and {stream_agg_instructions[-1]}."
        )
    else:
        text = f"Group rows by {x.group_by[0]} and for each group, {stream_agg_instructions[0]}."
    instruction = Instruction(text=text, idx=env["idx"])
    env["idx"] += 1
    return instructions + [instruction]


def index_scan_to_text(x: IndexScan, env: Environment) -> list[Instruction]:
    ordered = " in order" if x.ordered else ""
    predicates = (
        " by checking " + ", ".join(str(p) for p in x.predicates).replace("\\", "")
        if x.predicates
        else ""
    )
    instruction = Instruction(
        text=f"Scan {x.obj}{ordered}{predicates}.", idx=env["idx"]
    )
    env["idx"] += 1
    return [instruction]


def sort_to_text(x: Sort, env: Environment) -> list[Instruction]:
    instructions = relop_to_text(x.relop, env)
    sort_columns = [f"{cr.table}.{cr.column}" for cr in x.order_by.columns]
    order = "ascending" if x.order_by.ascending else "descending"
    distinct = "distinct" if x.distinct else ""
    instruction = Instruction(
        text=f"Sort {distinct} values {', '.join(sort_columns)} in {order} order.",
        idx=env["idx"],
    )
    env["idx"] += 1
    return instructions + [instruction]


def nested_loops_to_text(x: NestedLoops, env: Environment) -> list[Instruction]:
    left_ins = relop_to_text(x.left, env)
    left_idx = env["idx"] - 1
    right_ins = relop_to_text(x.right, env)
    right_idx = env["idx"] - 1
    pred = f" matching the condition: {x.predicate}" if x.predicate else ""
    instruction = Instruction(
        text=f"For each row in {left_idx}, scan {right_idx} and output rows{pred}.",
        idx=env["idx"],
    )
    env["idx"] += 1
    return left_ins + right_ins + [instruction]


def filter_to_text(x: Filter, env: Environment) -> list[Instruction]:
    instructions = relop_to_text(x.relop, env)
    instruction = Instruction(
        text=f"Restrict the set of rows based on {scalar_operator_to_text(x.predicate, env)}.",
        idx=env["idx"],
    )
    env["idx"] += 1
    return instructions + [instruction]


def top_sort_to_text(x: TopSort, env: Environment) -> list[Instruction]:
    instructions = relop_to_text(x.relop, env)
    order_by_columns = ", ".join(
        map(lambda c: apply_env(str(c), env), x.order_by.columns)
    )
    asc = "in ascending order" if x.order_by.ascending else "in descending order"
    columns = ", ".join(map(str, env["top_sort"][:-1]))
    instruction = Instruction(
        text=f"Sort {columns} by {order_by_columns} {asc} and take the top {x.rows} rows.",
        idx=env["idx"],
    )
    env["idx"] += 1
    return instructions + [instruction]


def top_to_text(x: Top, env: Environment) -> list[Instruction]:
    instructions = relop_to_text(x.relop, env)
    instruction = Instruction(
        text=f"Take the top {scalar_operator_to_text(x.top_expression)} rows.",
        idx=env["idx"],
    )
    env["idx"] += 1
    return instructions + [instruction]


def merge_to_text(x: Merge, env: Environment) -> list[Instruction]:
    left_ins = relop_to_text(x.left, env)
    left_idx = env["idx"] - 1
    right_ins = relop_to_text(x.right, env)
    right_idx = env["idx"] - 1
    instruction = Instruction(
        f"Merge the outputs of {left_idx} and {right_idx}.", idx=env["idx"]
    )
    env["idx"] += 1
    return left_ins + right_ins + [instruction]


def table_scan_to_text(x: TableScan, env: Environment) -> list[Instruction]:
    ordered = " in order" if x.ordered else ""
    predicates = (
        " by checking " + str(x.predicate).replace("\\", "") if x.predicate else ""
    )
    instruction = Instruction(text=f"Scan {x.obj}{ordered}{predicates}", idx=env["idx"])
    env["idx"] += 1
    return [instruction]


def hash_to_text(x: Hash, env: Environment) -> list[Instruction]:
    assert len(x.relops) in (1, 2)
    if len(x.relops) == 2:
        left, right = x.relops
        left_ins = relop_to_text(left, env)
        left_idx = env["idx"] - 1
        right_ins = relop_to_text(right, env)
        right_idx = env["idx"] - 1
    else:  # len(x.relops) == 1
        pass


def concat_to_text(x: Concat, env: Environment) -> list[Instruction]:
    instructions = [ins for relop in x.relops for ins in relop_to_text(relop, env)]
    instruction = "Concatenate the outputs of the previous {insno} nodes."
    return instructions + [instruction]


def row_count_spool_to_text(x: RowCountSpool, env: Environment) -> list[Instruction]:
    instructions = relop_to_text(x.relop, env)
    instruction = f"Count the number of rows in {env['idx']}."
    return instructions + [instruction]


def spool_to_text(x: Spool, env: Environment) -> list[Instruction]:
    return relop_to_text(x.relop, env)


def scalar_operator_to_text(x: ScalarOperator, env: Environment):
    if isinstance(x, Aggregate):
        if x.agg_type == "countstar":
            return "number of rows"
        return str(x)
    elif isinstance(x, Arithmetic):
        op = arith2sign[x.operation].replace("\\", "")
        lhs, rhs = [scalar_operator_to_text(s, env) for s in x.scalar_operators]
        return f"{lhs} {op} {rhs}"
    elif isinstance(x, Compare):
        op = comp2sign[x.compare_op].replace("\\", "")
        lhs, rhs = [scalar_operator_to_text(s, env) for s in x.scalar_operators]
        return f"{apply_env(lhs, env)} {op} {apply_env(rhs, env)}"
    elif isinstance(x, Const):
        return str(x.const_value)
    elif isinstance(x, Convert):
        return scalar_operator_to_text(x.scalar_operator, env)
    elif isinstance(x, If):
        pass
    elif isinstance(x, Identifier):
        return x.column_reference.column
    elif isinstance(x, Intrinsic):
        pass
    elif isinstance(x, Logical):
        pass
    else:
        raise ValueError(f"{type(x).__class__.__name__} does not exist.")


if __name__ == "__main__":
    from .ep_reader import get_train_dev_eps

    train, _ = get_train_dev_eps()
    for idx in (2520, 3207):
        ep = train[idx]
        print("=" * len(ep.query))
        print(ep.query)
        print("=" * len(ep.query))
        print(plan_to_text(ep))
        print(end="\n\n")
