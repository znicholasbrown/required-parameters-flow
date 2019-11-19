from prefect import Flow, task, Parameter
import urllib.request
from random import randrange

parameter_map = {}
words=[]

@task(name="Query for Words")
def words_query():
    global words
    if (len(words) > 0):
        return
    
    url = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"
    response = urllib.request.urlopen(url)
    text = response.read().decode()
    words = text.splitlines()

    words_length = len(words)

    print(f"\t50 random words from the dictionary: ")
    # we'll print 50 random words
    for i in range(50):
        random_index = randrange(words_length)
        print(f"\t\t{words[random_index]}")

def random_word():
    return words[randrange(len(words))]

@task(name="Print Required Parameters")
def print_params(task_params):
    print(f"\tPrinting required parameters!")
    for arg in task_params:
        print(arg)

@task(name="Make Required Parameters")
def make_params():
    global parameter_map
    print(f"\tMaking required parameters!")
    for i in range(50):
        word = random_word()
        val = randrange(10000)
        print(f"\t\tParameter {i}: {word}, {val}")
        parameter_map[i] = Parameter(word, default=val, required=True)


with Flow("Required Parameters Flow") as RequiredParameters_Flow:    
    query_task = words_query()

    make_task = make_params(upstream_tasks=[query_task])

    print_task = print_params(parameter_map, upstream_tasks=[query_task, make_task])
    print(print_task)


RequiredParameters_Flow.deploy(
    "Flow Schematics", 
    base_image="python:3.7",
    python_dependencies=[],
    registry_url="znicholasbrown",
    image_name="prefect_flow",
    image_tag="required-parameters-flow",
)