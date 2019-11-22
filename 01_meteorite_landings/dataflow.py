# Dataset: https://www.kaggle.com/nasa/meteorite-landings

import apache_beam as beam
from apache_beam import pipeline
from apache_beam.io.textio import ReadFromText
import json

def toJSON(record):
    yield json.loads(record)

def isValidYear(record):
    return record and 'year' in record

def isValidGeoLocation(record):
    return record and 'geolocation' in record

def convertYear(record):
   # print("r : %s" % record)
    record['year'] = int(record['year'][:4])
    return record

def filterYear(record):
    return 1800 <= record['year'] and record['year'] <= 2016 

def convertMass(record):
    if 'mass' in record:
        record['mass'] = float(record['mass'])
    else:
        record['mass'] = None
    return record

def convertGeoLocation(record):
    record['geolocation']['latitude']  = float(record['geolocation']['latitude'])
    record['geolocation']['longitude'] = float(record['geolocation']['longitude'])
    return record


def flattenGeoLocation(record):
    record.update(record['geolocation'])
    del record['geolocation']
    return record

def removeColumn(record):
    keys = {'recclass', 'mass', 'fall', 'year', 'latitude','longitude'}
    return { k: v for k, v in record.items() if k in keys }

def execute():
    with beam.Pipeline('DirectRunner') as p:
        (p 
            | 'ReadFile'                 >> ReadFromText(file_pattern='./t.jsonl')
            | 'toJson'                   >> beam.FlatMap(toJSON)
            | 'filterInvalidYear'        >> beam.Filter(isValidYear)
            | 'filterInvalidGeoLocation' >> beam.Filter(isValidGeoLocation)
            | 'convertYear'              >> beam.Map(convertYear)
            | 'filterYear'               >> beam.Filter(filterYear)
            | 'convertGeoLocation'       >> beam.Map(convertGeoLocation)
            | 'convertMass'              >> beam.Map(convertMass)
            | 'flattenGeoLocation'       >> beam.Map(flattenGeoLocation)
            | 'removeColumn'             >> beam.Map(removeColumn)
            | 'ParseJSON'                >> beam.Map(print) 
        )
    #p.run()

if __name__ == "__main__":
    execute()

