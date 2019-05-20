from flask import Flask
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from omim import import_omim_from_source, import_omim_onto_from_source
from orpha import import_disease_from_source, import_disease_clinical_sign_from_source

es = Elasticsearch()

omim_mdate = int(os.path.getmtime('Data/omim.txt'))
print('Last modification of OMIM: {}'.format(omim_mdate))

omim_last_update = es.search(index="source_updates", body={"query": {"term": {"source": "OMIM"}}})
print('Last indexation of OMIM: {}'.format(omim_last_update))

if res['hits']['total']['value'] == 0 || res['hits']['hits'][0]['last_update'] < omim_mdate:
	print('Reindex OMIM.')
	es.indices.delete(index='omim', ignore=[400, 404])

	records = import_omim_from_source('Data/omim.txt', ['TI', 'CS', 'NO'], ['NO'])
	actions = map(lambda record: {'_index': 'omim', '_type': '_doc', **record}, records)
    bulk(es, actions)

app = Flask(__name__)



@app.route('/gmd/api/disease/<name>')
def hello(name):
    return "Hello World!"