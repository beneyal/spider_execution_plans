import json
from typing import Literal, Tuple

from lxml import etree
from lxml.etree import _Element

from .ep_parser import parse
from .ep_types import ExecutionPlan


def read(split: Literal["train", "dev"]) -> list[_Element]:
    assert split in ("train", "dev"), 'The "split" parameter must be either "train" or "dev".'
    with open(f"dataset/{split}_spider_with_ep.json", encoding="utf-8") as f:
        dataset = json.load(f)
    return [etree.fromstring(ins["ep"]) for ins in dataset]


def get_train_dev_xmls() -> Tuple[list[_Element], list[_Element]]:
    return read("train"), read("dev")


def get_train_dev_eps() -> Tuple[list[ExecutionPlan], list[ExecutionPlan]]:
    train = [parse(ep) for ep in read("train")]
    dev = [parse(ep) for ep in read("dev")]
    return train, dev
