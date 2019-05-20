from flask import Flask
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from omim import import_omim_from_source, import_omim_onto_from_source
from orpha import import_disease_from_source, import_disease_clinical_sign_from_source

def clean_class_id(record):
    class_id = record['ClassId']
    class_id = class_id.split('/')[-1]
    class_id = class_id.split('.')[0]

    if class_id.startswith('MTHU'):
        class_id = class_id[4:]

    record['ClassId'] = int(class_id)

    return record

es = Elasticsearch()

print('Reindexing OMIM...', end='', flush=True)
es.indices.delete(index='omim', ignore=[400, 404])

records = import_omim_from_source('Data/omim.txt', ['TI', 'CS', 'NO'], ['NO'])
actions = map(lambda record: {'_index': 'omim', '_type': '_doc', **record}, records)
bulk(es, actions)

records = import_omim_onto_from_source('Data/omim_onto.csv', {'ClassId': 0, 'PreferredLabel': 1, 'Synonyms': 2, 'CUI': 5})
records = filter(lambda record: record['ClassId'].startswith('http://purl.bioontology.org/ontology/OMIM'), records)
records = map(clean_class_id, records)
actions = map(lambda record: {'_index': 'omim_onto', '_type': '_doc', **record}, records)
bulk(es, actions)

print('Done.')
print('Reindexing Orpha...', end='', flush=True)

records = import_disease_from_source('Data/disease.json')
actions = map(lambda record: {'_index': 'disease', '_type': '_doc', **record}, records)
bulk(es, actions)

records = import_disease_clinical_sign_from_source('Data/disease_clinical_sign.json')
actions = map(lambda record: {'_index': 'disease_clinical_sign', '_type': '_doc', **record}, records)
bulk(es, actions)

print('Done.')

app = Flask(__name__)



@app.route('/gmd/api/disease/<name>')
def hello(name):
    return "Hello World!"