from flask import Flask, jsonify
import urllib.parse
import pymongo
from flask_cors import CORS

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

client = pymongo.MongoClient('127.0.0.1', 27017)
db = client['gmd']

print('Reindexing OMIM...', end='', flush=True)

db.omim.drop()
records = import_omim_from_source('Data/omim.txt', ['TI', 'CS', 'NO'], ['NO'])
db.omim.insert_many(records)

db.omim_onto.drop()
records = import_omim_onto_from_source('Data/omim_onto.csv', {'ClassId': 0, 'PreferredLabel': 1, 'Synonyms': 2, 'CUI': 5})
records = filter(lambda record: record['ClassId'].startswith('http://purl.bioontology.org/ontology/OMIM'), records)
records = map(clean_class_id, records)
db.omim_onto.insert_many(records)

print('Done.')
print('Reindexing Orpha...', end='', flush=True)

db.disease.drop()
records = import_disease_from_source('Data/disease.json')
db.disease.insert_many(records)

db.disease_clinical_sign.drop()
records = import_disease_clinical_sign_from_source('Data/disease_clinical_sign.json')
db.disease_clinical_sign.insert_many(records)

print('Done.')


app = Flask(__name__)
CORS(app)


def orpha_disease_by_name(name):
	synonyms = set()
    omim_ids = set()
    orpha_ids = set()
    ulms_ids = set()

    disease_records = db.disease.find({'$or': [{'name': name}, {'synonyms': name}]}, {'_id': False})

    for record in disease_records:
    	synonyms.add(record['name'])
    	synonyms.update(record['synonyms'])

    	omim_ids.update(record['omim'])

    	orpha_ids.add(record['orpha'])

    	ulms_ids.update(record['ulms'])

    return {
    	'synonyms': synonyms,
    	'omim_ids': omim_ids,
    	'orpha_ids': orpha_ids,
    	'ulms_ids': ulms_ids
    }


def omim_disease_by_name(name):
	synonyms = set()
    omim_ids = set()
    orpha_ids = set()
    ulms_ids = set()

	return {
    	'synonyms': synonyms,
    	'omim_ids': omim_ids,
    	'orpha_ids': orpha_ids,
    	'ulms_ids': ulms_ids
    }


def omim_onto_disease_by_name(name):
	synonyms = set()
    omim_ids = set()
    orpha_ids = set()
    ulms_ids = set()

	return {
    	'synonyms': synonyms,
    	'omim_ids': omim_ids,
    	'orpha_ids': orpha_ids,
    	'ulms_ids': ulms_ids
    }


@app.route('/gmd/api/disease/<name>')
def disease(name):
    name = urllib.parse.unquote_plus(name)

    synonyms = set()
    omim_ids = set()
    orpha_ids = set()
    ulms_ids = set()

    results_by_name = [
    	orpha_disease_by_name(name),
    	omim_disease_by_name(name),
    	omim_onto_disease_by_name(name)
    ]

    for result in results_by_name:
    	synonyms.update(result['synonyms'])
    	omim_ids.update(result['omim_ids'])
    	orpha_ids.update(result['orpha_ids'])
    	ulms_ids.update(result['ulms_ids'])

    if(synonyms.contains(name)):
    	synonyms.remove(name)

	    return jsonify({
	    		'name': name,
	    		'synonyms': list(synonyms),
	    		'symptoms': [],
	    		'drugs': []
	    	})
	else:
		return jsonify({
				'name': '',
				'synonyms': [],
				'symptoms': [],
				'drugs': []
			})