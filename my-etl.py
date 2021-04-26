import bonobo
import requests
from bonobo.config import use_context_processor
import json

FABLABS_API_URL = 'https://dev.leaves.anant.us/solr/'

def extract_fablabs():
    yield from requests.get(FABLABS_API_URL).json().get('response').get('docs')

def with_opened_file(self, context):
    with open('output.txt', 'w+') as f:
        yield f

def normalize(*row):
    return row

@use_context_processor(with_opened_file)
def write_repr_to_file(f, *row):
    f.write(repr(row) + "\n")

def json_loader(filename):
    with open(filename, 'r') as fp:
        response_json = json.load(fp)
    yield from response_json.get('response').get('docs')

def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(normalize, write_repr_to_file, _input=None)
    graph.add_chain(extract_fablabs, bonobo.Limit(5), _output=normalize)
    graph.add_chain(json_loader('data.json'), bonobo.Limit(10), _output=normalize, _name="loadjson")
    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """
    return {}


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )