import json

from dataclasses import dataclass
from typing import Literal, Tuple

from lxml import etree
from lxml.etree import _Element

from .ep_parser import parse
from .ep_types import ExecutionPlan


def read(split: Literal["train", "dev"]) -> list[_Element]:
    assert split in (
        "train",
        "dev",
    ), 'The "split" parameter must be either "train" or "dev".'
    with open(f"dataset/{split}_spider_with_ep.json", encoding="utf-8") as f:
        return json.load(f)


def get_train_dev_xmls() -> Tuple[list[_Element], list[_Element]]:
    train = [etree.fromstring(ins["ep"]) for ins in read("train")]
    dev = [etree.fromstring(ins["ep"]) for ins in read("dev")]
    return train, dev


def get_train_dev_eps() -> Tuple[list[ExecutionPlan], list[ExecutionPlan]]:
    train, dev = get_train_dev_xmls()
    train_eps = [parse(ep) for ep in train]
    dev_eps = [parse(ep) for ep in dev]
    return train_eps, dev_eps


@dataclass(frozen=True)
class SpiderInstance:
    db_id: str
    query: str
    question: str
    ep: ExecutionPlan


def get_train_dev_spider_instances() -> Tuple[list[SpiderInstance], list[SpiderInstance]]:
    train = [SpiderInstance(ins["db_id"], ins["query"], ins["question"], parse(etree.fromstring(ins["ep"]))) for ins in read("train")]
    dev =[SpiderInstance(ins["db_id"], ins["query"], ins["question"], parse(etree.fromstring(ins["ep"]))) for ins in read("dev")] 
    return train, dev
