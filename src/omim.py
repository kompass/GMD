def import_omim_from_source(path, fields, numerics):
    record = None
    fieldname = None
    field = None

    with open(path) as reader:
        line = reader.readline()

        while line != '':
            if line.startswith('*RECORD*'):
                if record != None:
                    yield record

                record = {}
                fieldname = None
                field = None

            elif line.startswith('*FIELD*'):
                if fieldname != None and fieldname in fields:
                    if fieldname in numerics:
                        field = int(field[:-1])
                    record[fieldname] = field

                fieldname = line.split()[1]
                field = ''

            elif line.startswith('*THEEND*'):
                pass

            else:
                field += line

            line = reader.readline()

        if record != None:
            return record


def import_omim_onto_from_source(path, columns):
    CLASS_ID = 0
    PREFERRED_LABEL = 1
    SYNONYMS = 2
    CUI = 5

    with open(path) as reader:
        # Skip first line (header)
        reader.readline()

        line = reader.readline()

        while line != '':
            row = line.split(',')

            record = { column_name: row[column_id] for column_name, column_id in columns.items() }
            yield record

            line = reader.readline()



if __name__ == '__main__':
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
    es = Elasticsearch()

    # for record in import_omim_from_source('Data/omim.txt', ['TI', 'CS', 'NO'], ['NO']):
    #    es.index(index='omim', body=record)

    def clean_class_id(record):
        class_id = record['ClassId']
        class_id = class_id.split('/')[-1]
        class_id = class_id.split('.')[0]

        if class_id.startswith('MTHU'):
            class_id = class_id[4:]

        record['ClassId'] = int(class_id)

        return record

    records = import_omim_onto_from_source('Data/omim_onto.csv', {'ClassId': 0, 'PreferredLabel': 1, 'Synonyms': 2, 'CUI': 5})
    records = filter(lambda record: record['ClassId'].startswith('http://purl.bioontology.org/ontology/OMIM'), records)
    records = map(clean_class_id, records)

    actions = map(lambda record: {'_index': 'omim_onto', '_type': '_doc', **record}, records)
    bulk(es, actions)

    print('Done.')
