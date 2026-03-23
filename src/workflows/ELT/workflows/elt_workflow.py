from typing import Dict

from flytekit import workflow

from workflows.ELT.tasks.extract_load_task import extract_load_task


@workflow
def elt_workflow() -> Dict[str, str]:
    return extract_load_task()