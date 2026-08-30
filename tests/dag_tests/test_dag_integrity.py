from airflow.models import DagBag
import pytest
import os

DAG_FOLDER = os.environ.get("AIRFLOW_DAGS_FOLDER", "/opt/airflow/dags")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def pytest_generate_tests(metafunc):
    """Any test that takes `dag_id` gets auto-parametrized over every DAG found."""
    if "dag_id" in metafunc.fixturenames:
        bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag_ids = sorted(bag.dags.keys())
        metafunc.parametrize("dag_id", dag_ids, ids=dag_ids)


def test_no_import_errors(dagbag):  
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}."


def test_catchup_disabled(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    assert dag.catchup is False, f"DAG '{dag_id}' must explicitly set catchup=False."


def test_has_tags(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    assert dag.tags, f"DAG '{dag_id}' must have at least one tag defined."