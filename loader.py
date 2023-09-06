from random import randint
from datetime import datetime
from os import unlink, path as Path
import glob
from utils import *
import mysql.connector
import csv
from elasticsearch import Elasticsearch
# from urllib3 import disable_warnings
# disable_warnings()

DECODED_FOLDER = "/mnt/data/cdr/decoded"
ES_INDEX = "loaded_cdr"

MYSQL = dotdict( # this is a class i created in the utils file
    {
    "host": "mysql",
    "port": 3306,
    "user": "root",
    "password": "mapr",
    "database": "CDR"
})


def create_table(table_name):# Connect to the MySQL database
    db = mysql.connector.connect(
        host = MYSQL.host,
        port = MYSQL.port,
        user = MYSQL.user,
        password = MYSQL.password,
        database = MYSQL.database
    )

    # Create a cursor object to execute SQL queries
    cursor = db.cursor()

    # Define the table schema
    query = """
        CREATE TABLE IF NOT EXISTS CDR.table_name (
            id INT AUTO_INCREMENT PRIMARY KEY,
            type VARCHAR(10) NOT NULL,
            Flux VARCHAR(20) NOT NULL,
            TECHNO VARCHAR(20) NOT NULL,
            ID_Appelant BIGINT NOT NULL,
            ID_Destinataire BIGINT NOT NULL,
            Date_appel DATETIME NOT NULL,
            Duree VARCHAR(20) NOT NULL,
            Region VARCHAR(50) NOT NULL,
            Status VARCHAR(30) NOT NULL,
            col9 INT NOT NULL,
            col10 INT NOT NULL,
            col11 INT NOT NULL,
            filename VARCHAR(100) NOT NULL
        );
    """

    # Execute the CREATE TABLE statement
    cursor.execute(query.replace("table_name", table_name))

    # Close the database connection
    db.close()

def insert_rows(table_name, rows):
    # Connect to the MySQL database
    db = mysql.connector.connect(
        host = MYSQL.host,
        port = MYSQL.port,
        user = MYSQL.user,
        password = MYSQL.password,
        database = MYSQL.database
    )

    # Create a cursor object to execute SQL queries
    cursor = db.cursor()

    # Define the insert query :
    # query = """
    # INSERT INTO CDR.table_name values ();
    # """
    # print(rows[0])

    sql = "INSERT INTO CDR.table_name (type, Flux, TECHNO, ID_Appelant, ID_Destinataire, Date_appel, Duree, Region, Status, col9, col10, col11, filename) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".replace("table_name", table_name)
    cursor.executemany(sql, rows)

    # Commit the changes to the database
    db.commit()

    # Close the database connection
    db.close()



if __name__ == "__main__":

    es = Elasticsearch(['http://elasticsearch.monitoring.svc.cluster.local:9200'])
#     es = Elasticsearch(
#     ['https://elasticsearch-master.monitoring.svc.cluster.local:9200'],
#     http_auth=('elastic', 'ArNMA3e7ct0fuvdT'),
#     verify_certs=False
# )
    # es = Elasticsearch(['http://elasticsearch-master-headless.default.svc.cluster.local:9200'],http_auth=('elastic', 'hpTxdiShRYlyXrMt'))
    # es=Elasticsearch([{"host":"elasticsearch-master-headless.default.svc.cluster.local","port":9200}],http_auth=('elastic', 'slzIsnsUD2hicTPF'))

    print(f"{datetime.now()} - Loading files into MySQL")
    current = datetime.now().strftime('%Y-%m-%d  %H')
    date = current.split("  ")[0]
    heure = current.split("  ")[1]

    # create mysql tables if they don't exist
    for table in [d["table"] for d in data]:
        print(f"{datetime.now()} - Checking table : {table}")
        create_table(table)
    
    # get all files in the decoded folder
    files = glob.glob(f"{DECODED_FOLDER}/*/*.csv.done", recursive=True)
    # data_all= []
    # result=[]
    # loop through files
    print(f"{datetime.now()} - Found {len(files)} files")
    for file in files:
        print(f"{datetime.now()} - Processing file : {file}")
        # get the lines 
        rows = []
        
        folder = Path.dirname(file).replace("\\", "/").split("/")[-1]
        filename = file.replace("\\", "/").split("/")[-1].replace(".done", "")
        table_name = [d["table"] for d in data if d["chemin"] == folder][0]
        
        with open(file, "r", encoding="utf-8") as f:
            # lines = f.readlines()   
            reader = csv.reader(f)
            # Loop through the rows in the CSV file

            for row in reader:
                # Append the row to the list
                row.append(filename)
                rows.append(tuple(row))
        #print(rows)
        numlines = len(rows)
        # print("numlines : ", numlines)
        to_delete = 0
        # je veux que ~5% des fichiers soient corrompus au chargement dans MySQL: nombre de lignes dans le fichier DECODE est different du nombre de lignes dans le fichier RAW
        if randint(0, 20) == 0:
            # dans ce cas, on enleve entre 500 et 1000 lignes :
            to_delete = randint(500, 1000)
            rows = rows[:-to_delete]

        # print("table_name : ", table_name)
        print(f"{datetime.now()} - Inserting {len(rows)} rows into table : {table_name}")
        insert_rows(table_name, rows)
        print(f"{datetime.now()} - Done inserting rows into table : {table_name}")


        print(f"{datetime.now()} - Indexing into elasticsearch")
        folder = file.replace("\\", "/").split("/")[-2]
        flux =  [d["flux"] for d in data if d["chemin"] == folder][0]
        techno =  [d["techno"] for d in data if d["chemin"] == folder][0]
        doc = {
            "type": "DECODED",
            "date": date,
            "heure": heure,
            "count": numlines - to_delete,
            "flux": flux,
            "techno": techno,
            "path": filename,
            "executed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # result.append(doc)
        # data_all +=  result
        resp = es.index(index=ES_INDEX, document=doc)

        # delete the raw file
        print(f"{datetime.now()} - Deleting {file}...")
        try:
            unlink(file)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file, e))

        print(f"{datetime.now()} - Indexing into elasticsearch done. {resp['result']}")
        
    print(f"{datetime.now()} - Done Loading files into MySQL")


# from random import randint
# from datetime import datetime
# from os import unlink, path as Path
# import glob
# from utils import *
# import mysql.connector
# import csv 

# DECODED_FOLDER = "/mnt/data/cdr/decoded"
# # DECODED_FOLDER = "C:/Users/HP/Desktop/Volume/CDR/decoded"

# MYSQL = dotdict( # this is a class i created in the utils file
#     {
#     "host": "mysql",
#     "port": 3306,
#     "user": "root",
#     "password": "mapr",
#     "database": "CDR"
# })


# def create_table(table_name):# Connect to the MySQL database
#     db = mysql.connector.connect(
#         host = MYSQL.host,
#         port = MYSQL.port,
#         user = MYSQL.user,
#         password = MYSQL.password,
#         database = MYSQL.database
#     )

#     # Create a cursor object to execute SQL queries
#     cursor = db.cursor()

#     # Define the table schema
#     query = """
#         CREATE TABLE IF NOT EXISTS CDR.table_name (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             col0 VARCHAR(10) NOT NULL,
#             col1 INT NOT NULL,
#             col2 INT NOT NULL,
#             col3 INT NOT NULL,
#             col4 INT NOT NULL,
#             col5 BIGINT NOT NULL,
#             col6 BIGINT NOT NULL,
#             col7 BIGINT NOT NULL,
#             col8 VARCHAR(10) NOT NULL,
#             col9 INT NOT NULL,
#             col10 INT NOT NULL,
#             col11 INT NOT NULL,
#             filename VARCHAR(100) NOT NULL
#         );
#     """

#     # Execute the CREATE TABLE statement
#     cursor.execute(query.replace("table_name", table_name))

#     # Close the database connection
#     db.close()

# def insert_rows(table_name, rows):
#     # Connect to the MySQL database
#     db = mysql.connector.connect(
#         host = MYSQL.host,
#         port = MYSQL.port,
#         user = MYSQL.user,
#         password = MYSQL.password,
#         database = MYSQL.database
#     )

#     # Create a cursor object to execute SQL queries
#     cursor = db.cursor()

#     # Define the insert query :
#     # query = """
#     # INSERT INTO CDR.table_name values ();
#     # """
#     # print(rows[0])

#     sql = "INSERT INTO CDR.table_name (col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, filename) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".replace("table_name", table_name)
#     cursor.executemany(sql, rows)

#     # Commit the changes to the database
#     db.commit()

#     # Close the database connection
#     db.close()



# if __name__ == "__main__":


#     print(f"{datetime.now()} - Loading files into MySQL")
#     current = datetime.now().strftime('%Y-%m-%d  %H')
#     date = current.split("  ")[0]
#     heure = current.split("  ")[1]

#     # create mysql tables if they don't exist
#     for table in [d["table"] for d in data]:
#         print(f"{datetime.now()} - Checking table : {table}")
#         create_table(table)
    
#     # get all files in the decoded folder
#     files = glob.glob(f"{DECODED_FOLDER}/*/*.csv.done", recursive=True)

#     # loop through files
#     print(f"{datetime.now()} - Found {len(files)} files")
#     for file in files:
#         print(f"{datetime.now()} - Processing file : {file}")
#         # get the lines 
#         rows = []
        
#         folder = Path.dirname(file).replace("\\", "/").split("/")[-1]
#         filename = file.replace("\\", "/").split("/")[-1].replace(".done", "")
#         table_name = [d["table"] for d in data if d["chemin"] == folder][0]
        
#         with open(file, "r", encoding="utf-8") as f:
#             # lines = f.readlines()   
#             reader = csv.reader(f)
#             # Loop through the rows in the CSV file
#             for row in reader:
#                 # Append the row to the list
#                 row.append(filename)
#                 rows.append(tuple(row))
#         # print(rows)
#         numlines = len(rows)
#         # print("numlines : ", numlines)
#         to_delete = 0
#         # je veux que ~5% des fichiers soient corrompus au chargement dans MySQL: nombre de lignes dans le fichier DECODE est different du nombre de lignes dans le fichier RAW
#         if randint(0, 20) == 0:
#             # dans ce cas, on enleve entre 500 et 1000 lignes :
#             to_delete = randint(500, 1000)
#             rows = rows[:-to_delete]

#         # print("table_name : ", table_name)
#         print(f"{datetime.now()} - Inserting {len(rows)} rows into table : {table_name}")
#         insert_rows(table_name, rows)
#         print(f"{datetime.now()} - Done inserting rows into table : {table_name}")


#         print(f"{datetime.now()} - Indexing into elasticsearch")
#         folder = file.replace("\\", "/").split("/")[-2]
#         flux =  [d["flux"] for d in data if d["chemin"] == folder][0]
#         techno =  [d["techno"] for d in data if d["chemin"] == folder][0]
#         doc = {
#             "type": "DECODED",
#             "date": date,
#             "heure": heure,
#             "count": numlines - to_delete,
#             "flux": flux,
#             "techno": techno,
#             "path": filename,
#             "executed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         }
#         # resp = es.index(index=ES_INDEX, document=doc)

#         #delete the raw file
#         print(f"{datetime.now()} - Deleting {file}...")
#         try:
#             unlink(file)
#         except Exception as e:
#             print('Failed to delete %s. Reason: %s' % (file, e))

#         # print(f"{datetime.now()} - Indexing into elasticsearch done. {resp['result']}")
        
#     print(f"{datetime.now()} - Done Loading files into MySQL")

