import json
import psycopg2 as pg
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine

schema_json  = '/home/fajar/Documents/project_3_de/sql/schemas/user_address.json'
create_schema_sql = """create table user_address_master {}; """
zip_medium_file = '/home/fajar/Documents/project_3_de/temp/dataset-medium.zip'
medium_file_name = 'dataset-medium.csv'
result_ingestion_check_sql = '/home/fajar/Documents/project_3_de/sql/queries/result_ingestion_user_address_medium.sql'
########################################################################################################################
#file connection to postgresql
database = 'shipping_orders'
user = 'postgres'
password = 'fajarsetia'
host = '127.0.0.1'
port = '5432'
table_name = 'user_address_master'
########################################################################################################################
with open (schema_json, 'r') as schema: #membuka dan membaca file .json
    content = json.loads(schema.read())

#buat variable list kosong dengan nama list_schema
#lakukan looping dengan membuat variabel yang isinya value dari file .json
#buat variabel ddl_list yang menampung semua value data
#lakukan proses append list_schema dan ddl_list

list_schema = []
for c in content :
    col_name = c['column_name']
    col_type = c['column_type']          # melakukan perulangan data
    constraint = c['is_null_able']       # mengambil data json
    ddl_list = [col_name, col_type, constraint]
    list_schema.append(ddl_list)
   
list_schema_2 = []
for f in list_schema: # perulangan data
                      # melakukan join data dengan pemisah berupa spasi
    s = ' '.join(f)
    list_schema_2.append(s)

# merubah bentuk data list menjadi tuple dan 
# menghilangkan tanda petik satu dengan fungsi replace
create_schema_final = create_schema_sql.format(tuple(list_schema_2)).replace("'","")

#config connecting to postgres
conn = pg.connect(
    database = database,
    user = user,
    host = host,
    password = password,
    port = port
)
conn.autocommit = True  #perintah commit secara otomatis
cursor = conn.cursor()

try:
    cursor.execute(create_schema_final)
    print("DDL schema created succesfully..")
except pg.errors.DuplicateTable:
    print("Table Already created..")    

# Load zip file to Dataframe
zip_file = ZipFile(zip_medium_file) # extract file zip
df = pd.read_csv(zip_file.open(medium_file_name), chunksize = 30000, header = None) # open and read file zip

# create engine (import create engine)
engine = create_engine (f'postgresql://{user}:{password}@{host}:{port}/{database}')


part = 0
for chunk in df:

    col_name_df = [c['column_name'] for c in content]
    chunk.columns = col_name_df  # memasukkan isi data col_name_df berupa (first_name, last_name
                            #address, created_at) ke dalam columns df untuk di jadikan header   

    # insert into postgresql                                         
    part += 1
    if part == 1:
        chunk.to_sql(table_name, engine, if_exists ='replace', index = False)
        
    else: 
        chunk.to_sql(table_name, engine, if_exists = 'append', index = False)

    print (chunk)
    print(f'data part-{part}')

    cursor.execute(open(result_ingestion_check_sql, 'r').read())
    result = cursor.fetchall()
    # membaca file result_ingestion_user_address_medium.sql
    print(f'Min created_at: {result[0][1]}') #tanggal created terkecil             
    print(f'Max created_at: {result[0][2]}') #tanggal created terbesar
    print(f'Jumlah data antara tanggal 2018-02-01 dan tanggal 2018-12-24: {result[0][0]}') #jumlah data yang masuk
                                             



