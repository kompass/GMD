import json

def import_disease_from_source(path):
    with open(path) as reader:
        j = json.load(reader)

        for row in j['rows']:
            name = row['value']['Name']['text']

            link = row['value']['ExpertLink']['text']
            
            synonyms = []
            syn_count = int(row['value']['SynonymList']['count'])
            if  syn_count == 1:
                synonyms.append(row['value']['SynonymList']['Synonym']['text'])
            elif syn_count != 0:
                for synonym in row['value']['SynonymList']['Synonym']:
                    synonyms.append(synonym['text'])

            yield {'name': name, 'link': link, 'synonyms': synonyms}


def import_disease_clinical_sign_from_source(path):
    with open(path) as reader:
        j = json.load(reader)

        for row in j['rows']:
            disease = row['value']['disease']['Name']['text']

            clinicalSign = row['value']['clinicalSign']['Name']['text']

            yield {'disease': disease, 'clinicalSign': clinicalSign}

if __name__ == '__main__':
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
    es = Elasticsearch()

    # records = import_disease_from_source('Data/disease.json')
    # actions = map(lambda record: {'_index': 'disease', '_type': '_doc', **record}, records)
    # bulk(es, actions)

    records = import_disease_clinical_sign_from_source('Data/disease.json')
    actions = map(lambda record: {'_index': 'disease_clinical_sign', '_type': '_doc', **record}, records)
    bulk(es, actions)

    print('Done.')
