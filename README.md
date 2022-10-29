# Spider Execution Plans

## Instructions for Using Execution Plans

The Spider execution plans are already present in the train and development splits in the `dataset` folder. In order to work with them, do the following:

1. Clone this repository.
2. Create a new Python 3.9 virtual environment.
3. Run `pip install -r requirements.txt`.
4. In order to read Spider instances, use the `get_train_dev_spider_instances()` function in the `ep_reader.py` module. The `SpiderInstance` dataclass is also defined there:

```python
@dataclass(frozen=True)
class SpiderInstance:
    db_id: str
    query: str
    question: str
    ep: ExecutionPlan
```
The `ExecutionPlan` type and its components are defined in `ep_types.py`.