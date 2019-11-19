from prefect import Flow, task, Parameter
import urllib.request
from random import randrange

@task(name="Print Required Parameters")
def print_params(task_params):
    print(f"\tPrinting required parameters!")
    for arg in task_params:
        print(arg)

with Flow("Required Parameters Flow") as RequiredParameters_Flow:
    parameter_map = {}

    print(f"\tGenerating required parameters")
    for i in range(250):
        print(f"\t\tParameter {i}: default_{i}")
        parameter_map[i] = Parameter(f"Parameter_{i}", required=True)

    print_task = print_params(parameter_map, upstream_tasks=[])
    print(print_task)


RequiredParameters_Flow.deploy(
    "Flow Schematics", 
    base_image="python:3.7",
    python_dependencies=[],
    registry_url="znicholasbrown",
    image_name="prefect_flow",
    image_tag="required-parameters-flow",
)