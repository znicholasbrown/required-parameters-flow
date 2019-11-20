import prefect
from prefect import Parameter, Flow, task

p, q = Parameter("default_param", default=None), Parameter("required_param", required=True)

@task
def type_check(param):
    prefect.context.logger.info(f"Received val {p} of type {t}".format(p=param, t=type(param)))

with Flow("Parameter Type Testing") as Parameter_Type_Testing:
    type_check.map([p, q])

Parameter_Type_Testing.deploy(
    "Flow Schematics",
    base_image="python:3.7",
    python_dependencies=[],
    registry_url="znicholasbrown",
    image_name="prefect_flow",
    image_tag="parameter-type-testing-flow",
)