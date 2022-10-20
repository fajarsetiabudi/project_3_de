import json
import psycopg2 as pg #module untuk eksekusi query postgres di python pip install psycopg2-binary
from zipfile import ZipFile #pip install zipfile36
import pandas as pd
from sqlalchemy import create_engine

schema_json = '/home/fajar/Documents/project_3_de/sql/schemas/user_address.json'
create_schema_sql = """create table user_address_2018_snapshots {};"""
zip_small_file = '/home/fajar/Documents/project_3_de/temp/dataset-small.zip'
small_file_name = 'dataset-small.csv'
result_ingestion_check_sql = '/home/fajar/Documents/project_3_de/sql/queries/result_ingestion_user_address.sql'

database='shipping_orders'
user='postgres'
password='fajarsetia'
host='127.0.0.1'
port='5432'
table_name = 'user_address_2018_snapshots'

with open (schema_json, 'r') as schema: #membuka dan membaca file .json
    content = json.loads(schema.read())

#buat variable list kosong dengan nama list_schema
#lakukan looping dengan membuat variabel yang isinya value dari file .json
#buat variabel ddl_list yang menampung semua value data
#lakukan proses append list_schema dan ddl_list

list_schema = []
for c in content:
     col_name = c['column_name']
     col_type = c['column_type']
     constraint = c['is_null_able']
     ddl_list = [col_name, col_type, constraint]
     list_schema.append(ddl_list)

list_schema_2 = []
for l in list_schema: # perulangan data
     s = ' '.join(l)  # melakukan join data dengan pemisah berupa spasi
     list_schema_2.append(s) 

# merubah bentuk data list menjadi tuple dan 
# menghilangkan tanda petik satu dengan fungsi replace
create_schema_sql_final = create_schema_sql.format(tuple(list_schema_2)).replace("'", "")

#Konfigurasi connecting postgres
conn = pg.connect(database=database,
                  user=user,
                  password=password,
                  host=host,
                  port=port)

conn.autocommit=True #perintah commit secara otomatis
cursor=conn.cursor()

try:
    cursor.execute(create_schema_sql_final)
    print("DDL schema created succesfully...") #penanganan error dengan try - except
except pg.errors.DuplicateTable:
    print("table already created...")

#Load zipped file to dataframe

zf = ZipFile(zip_small_file) #extract file zip
df = pd.read_csv(zf.open(small_file_name), header = None) #open/read file zip

col_name_df = [c['column_name'] for c in content] # open file 
df.columns = col_name_df # memasukkan isi data col_name_df berupa (first_name, last_name
                         #address, created_at) ke dalam columns df untuk di jadikan header

# filter tanggal 
df_filtered = df[(df['created_at'] >= '2018-02-01') & (df['created_at'] < '2018-12-31')]

#create engine (import create_engine)
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

#insert to postgres
df_filtered.to_sql(table_name, engine, if_exists='replace', index=False) 

print(f'Total inserted rows: {len(df_filtered)}') #melihat jumlah data masuk menggunakan fungsi len
print(f'Inital created_at: {df_filtered.created_at.min()}') #tanggal created terkecil
print(f'Last created_at: {df_filtered.created_at.max()}') # tanggal created terbesar

cursor.execute(open(result_ingestion_check_sql, 'r').read())
result = cursor.fetchall()  # membaca file result_ingestion_user_address.sql
print(result)               #dan menampilkan data berdasarkan perintah di dalam 
                            #file result_ingestion_user_address.sql
