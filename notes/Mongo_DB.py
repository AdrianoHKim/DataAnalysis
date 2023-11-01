'''!pip install gcsfs  # para usar gcloud

# modo 1 pymongo
!pip install pymongo
'''

# modo 2 pymongo
#!python -m pip install pymongo

# Abertura de bibliotecas de manipulação e análise
import pandas as pd
import numpy as np


# Abertura de bibliotecas de conectores
import os
from google.cloud import storage
from pymongo import MongoClient


# Usando o Google Drive

# Comando de abertura para Google Drive
#from google.colab import drive
#drive.mount('/content/drive')


# Extração por Google Drive
#df = pd.read_csv('/content/drive/MyDrive/Semana 6 - Python para Análise de Dados/cenipa_bruto.csv',
#                 sep=';',
#                 encoding='ISO-8859-1',
#                 dayfirst = True)


# Usando o GCP

# Documentação: https://cloud.google.com/docs/authentication?hl=pt-br

# CONFIGURANDO DA CHAVE DE SEGURANCA - ACESSO O PROJETO
serviceAccount = 'chave json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = serviceAccount

# Configurações Google Cloud Storage - ACESSO AO BUCKET
client = storage.Client()
# nome da bucket
bucket = client.get_bucket('bucket_do_kimzera')
# nome do arquivo
bucket.blob('airbnb.csv')
path = 'gs://bucket_do_kimzera/projeto_airbnb/raw/airbnb.csv'  # gsuti do arquivo


# Abertura da base de dados e cópia de segurança (bucket)
df = pd.read_csv(path,
                 sep=';',
                 encoding='ISO-8859-1',
                 dayfirst = True)
dfback = df.copy()


# Usando o Mongo DB

# O MEU URI QUE APARECE LA !
uri = "mongodb+srv://cluster0.dvu2vvd.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
client = MongoClient(uri,
                     tls=True,
                     tlsCertificateKeyFile='/content/X509-cert-2899292319393237680.pem')  # O MEU CERTIFICADO
#                     server_api=ServerApi('1'))  nao usaremos API
db = client['testDB']
collection = db['testCol']
doc_count = collection.count_documents({})
print(doc_count)


# Conector MongoDB
# USAR O MESMO URI LA DE CIMA!!!!!
uri = "mongodb+srv://cluster0.dvu2vvd.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority"
client = MongoClient(uri, tls=True, tlsCertificateKeyFile='/content/X509-cert-2899292319393237680.pem')


# Escolhendo a base de dados e coleção
db = client['pandasmongo']
collection = db['brutos']


# Contagem dos documentos
doc_count = collection.count_documents({})
print(doc_count)


# Abertura da base de dados e cópia de segurança (bucket)
df = pd.read_csv(path,
                 sep=';',
                 encoding='ISO-8859-1',
                 dayfirst = True)
dfback = df.copy()



# Conversão para colocar no MongoDB
df_dict = df.to_dict("records")
collection.insert_many(df_dict)

# Checagem de valores no MongoDB
collection.count_documents({})


# Checagem da coleção do MongoDB
for x in collection.find():
  print(x)

# Exemplo de tratamento

path = 'gs://bucket_do_kimzera/projeto_airbnb/refined/airbnb_tratado.csv'   # GSUTI DO ARQUIVO TRATADO DA BUCKET
df = pd.read_csv(path,
                 sep=',',
                 encoding='ISO-8859-1',
                 )

# Carregamento

# Exportação no Google Drive.
#df.to_csv('cenipa_tratado.csv', index=False)


# Google Cloud
#df.to_csv('gs://projetos-aula/projeto-cenipa/tratado/cenipa_tratado.csv', index=False) # salva no bucket


# Carregamento no MongoDB
db2 = client['pandasmongo']
collection2 = db2['tratados']
collection2.count_documents({})


# Conversão de dados para MongoDB
df_dict = df.to_dict("records")
collection2.insert_many(df_dict)

# Contagem de dados: verificação
collection2.count_documents({})


# Checagem da coleção
for x in collection2.find():
  print(x)
