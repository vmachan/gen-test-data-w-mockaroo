import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import uuid
import redshift_connector as rc
import psycopg2
import os
import re
import json

from mockaroo import Client

out_num_rows=1000
out_fmt='csv'
out_delimiter=','
out_quote_char='"'
out_line_ending='unix'
out_file_loc=r'./'

mockaroo_api_key = os.getenv('MOCKAROO_API_KEY')
mockaroo_datasets_url = "https://api.mockaroo.com/api/datasets/"
mockaroo_client = Client(api_key=mockaroo_api_key)

rs_host=os.getenv('REDSHIFT_HOST')
rs_db=os.getenv('REDSHIFT_DB')
rs_port=os.getenv('REDSHIFT_PORT')
rs_user=os.getenv('REDSHIFT_USER')
rs_pswd=os.getenv('REDSHIFT_PSWD')

get_this_schema = 'rsa_landing'

rs_to_mck_dtyp_map = {
    # re.compile(r'.*CHAR.*', re.IGNORECASE): '\'Character Sequence\', \'format\': \'**********************\'',
    re.compile(r'.*CHAR.*', re.IGNORECASE): 'Character Sequence',
    re.compile(r'.*TIMESTAMP.*', re.IGNORECASE): 'Datetime',
    re.compile(r'.*DATE.*', re.IGNORECASE): 'Datetime',
    re.compile(r'.*INTEGER.*', re.IGNORECASE): 'Number',
    re.compile(r'.*BIGINT.*', re.IGNORECASE): 'Number',
    re.compile(r'.*SMALLINT.*', re.IGNORECASE): 'Number',
    re.compile(r'.*NUMERIC.*', re.IGNORECASE): 'Number',
    re.compile(r'.*BOOLEAN.*', re.IGNORECASE): 'Boolean',
}

columns_metadata = { "name" : '', "type": '', "format": '' }
tables_metadata = { "table_name" : '', "columns": [] }

schema_metadata = { "schema_name": get_this_schema, "tables": [] }

def map_dtyp(in_dtyp):
    for pattern, mapped_type in rs_to_mck_dtyp_map.items():
        if pattern.match(in_dtyp):
            return mapped_type
    return in_dtyp

# Given a schema name get Redshift metadata
def get_rs_metadata(schema_name):
    conn = psycopg2.connect(
         host=rs_host,
         database=rs_db,
         port=rs_port,
         user=rs_user,
         password=rs_pswd
      )
  
    rs_re_sql = "SELECT tablename, column_name, column_type FROM public.vm_get_rs_metadata \
                  WHERE schemaname = '" + schema_name + "'"
    # Create a Cursor object
    cursor = conn.cursor()

    # Query a table using the Cursor
    cursor.execute(rs_re_sql)
    rs_metadata = cursor.fetchall()

    # print(schema_metadata['schema_name'])
    # print(schema_metadata['tables'])
    # print(type(schema_metadata['tables']))

    previous_table = ''
    for metadata in rs_metadata:
        table_name = metadata[0]
        column_name = metadata[1].strip('"').strip("'")
        column_type = metadata[2]
        # print("RS: " + schema_name + "." + table_name + "." + column_name + "." + column_type)
        if (previous_table != table_name):
            if (previous_table != ''):
                None
                # print("]")
            # print("schm_" + table_name + " = [")
            single_table_metadata = { "table_name": table_name, "columns": [] }
            schema_metadata["tables"].append(single_table_metadata)
            curr_table_idx = len(schema_metadata["tables"]) - 1
        # print("{ 'name': '" + column_name + "', 'type' : '" + map_dtyp(column_type) + "' }")
        single_column_metadata = { "name": column_name, "type": map_dtyp(column_type) }
        if (map_dtyp(column_type) == "Character Sequence"):
            single_column_metadata["format"] = "*********************"
        if not("primary" in column_name.lower() or "alter" in column_name.lower()):
            schema_metadata["tables"][curr_table_idx]["columns"].append(single_column_metadata)
        previous_table = table_name
    # print("]")

def mck_data(table_name, columns):
    # print(columns)
    data = mockaroo_client.generate(
        fields=columns
        ,count=out_num_rows
        ,fmt=out_fmt
        ,delimiter=out_delimiter
        ,quote_char=out_quote_char
        ,line_ending=out_line_ending
    )
    # print(data.decode())

get_rs_metadata(get_this_schema)
# print(schema_metadata)
# print(json.dumps(schema_metadata))

for table in schema_metadata["tables"]:
    print("Table: " + table["table_name"])
    # print("Columns ----------- : ", table["columns"])
    mck_data(table["table_name"], table["columns"])
    # input()

