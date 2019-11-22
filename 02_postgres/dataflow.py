import apache_beam as beam
from apache_beam import pipeline
from beam_nuggets.io import relational_db

def execute():
    with beam.Pipeline('DirectRunner') as p:
        source_config = relational_db.SourceConfiguration(
            drivername='postgresql+pg8000',
            host='localhost',
            port=5432,
            username='postgres',
            password='secret',
            database='postgres',
        )
        (p
            | 'Read from db table' >> relational_db.ReadFromDB(source_config=source_config, table_name='my_data', query='select id, my_attribute from my_data')
            | 'Writing to stdout'  >> beam.Map(print)
        )

if __name__ == "__main__":
    execute()

