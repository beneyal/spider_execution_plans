from .ep_reader import get_train_dev_eps
from .ep_types import *


def query(ep: ExecutionPlan, q, **kwargs):
    result = []
    stack = [ep.relop]
    while stack:
        relop = stack.pop()
        op = relop.operation

        if isinstance(op, q) and all([getattr(op, k) == v for k, v in kwargs.items()]):
            result.append(op)

        if hasattr(op, "relop"):
            stack.append(op.relop)
        elif hasattr(op, "relops"):
            stack.extend(op.relops)
        elif hasattr(op, "left"):
            stack.extend([op.left, op.right])

    return result


def query_all(q, **kwargs):
    train, dev = get_train_dev_eps()
    train_results = [result for ep in train for result in query(ep, q, **kwargs)]
    dev_results = [result for ep in dev for result in query(ep, q, **kwargs)]
    return {
        "train": train_results,
        "dev": dev_results
    }


if __name__ == '__main__':
    print(query_all(Sort))
