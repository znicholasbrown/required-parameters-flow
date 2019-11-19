from prefect import Flow, task, Parameter
import urllib.request
from random import uniform

parameter_map = {}
words=[]

@task(name="Query for Words")
def words_query():
    if (len(words) > 0):
        return words
    
    url = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"
    response = urllib.request.urlopen(url)
    text = response.read().decode()
    words = text.splitlines()

@task(name="Get Random Words")
def random_words():
    return words[uniform(0, len(words))]

@task(name="Print Required Parameters")
def print_params(task_params):
    for arg in task_params:
        print(arg)

@task(name="Make Required Parameters")
def make_params():
    for i in range(50):
        parameter_map[i] = Parameter(random_words(), default=uniform(0, 100000), required=True)


with Flow("Required Parameters Flow") as RequiredParameters_Flow:
    query_task = words_query()

    make_task = make_params(upstream_tasks=[query_task])

    print_task = print_params(parameter_map, upstream_tasks=[query_task, make_task])
    


RequiredParameters_Flow.deploy(
    "Flow Schematics", 
    base_image="python:3.7",
    python_dependencies=["random"],
    registry_url="znicholasbrown",
    image_name="prefect_flow",
    image_tag="required-parameters-flow",
)