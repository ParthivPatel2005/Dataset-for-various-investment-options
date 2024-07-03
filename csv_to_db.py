import pymongo
import pandas as pd
import numpy as np

def to_float(s):
    try:
        s = str(s).split(',')
        s = ''.join(s)

        return float(s)
    except:
        return np.nan

client_mongo = pymongo.MongoClient('mongodb://localhost:27017/')
db = client_mongo['delta_simulator']
collection_mutual_funds = db['mutual_funds']
collection_etfs = db['etfs']
collection_crypto = db['crypto']
collection_gold_bonds = db['gold_bonds']
collection_bonds = db['bonds']

df_mf = pd.read_csv('mutual_funds.csv')
df_mf.drop(columns=df_mf.columns[0], inplace=True)
df_etfs = pd.read_csv('etfs.csv')
for i in range(18):
    column_name = df_etfs.columns[i]
    df_etfs.columns.values[i] = column_name[:-2].strip()
for i in [2,3,4,5,6,7,8,9,10,11,12,13,14,16]:
    df_etfs[df_etfs.columns[i]] = df_etfs[df_etfs.columns[i]].apply(to_float)
df_crypto = pd.read_csv('crypto_data.csv')
df_crypto.drop(columns=df_crypto.columns[0], inplace=True)
df_bonds = pd.read_excel('List_of_securitites.xlsx')
df_bonds.drop(columns=df_bonds.columns[0], inplace=True)
df_bonds['Coupon Rate (%)'] = df_bonds['Coupon Rate (%)'].apply(to_float)
df_gold_bonds = pd.read_csv('gold_bonds.csv')
for i in [1,2,3,4,5,6,7,8,9,10,11,12,13,15]:
    df_gold_bonds[df_gold_bonds.columns[i]] = df_gold_bonds[df_gold_bonds.columns[i]].apply(to_float)

list_mfs = [{df_mf.columns[i]: df_mf[df_mf.columns[i]][j] for i in range(len(df_mf.columns))} for j in range(len(df_mf))]
list_etfs = [{df_etfs.columns[i]: df_etfs[df_etfs.columns[i]][j] for i in range(len(df_etfs.columns))} for j in range(len(df_etfs))]
list_cryptos = [{df_crypto.columns[i]: df_crypto[df_crypto.columns[i]][j] for i in range(len(df_crypto.columns))} for j in range(len(df_crypto))]
list_bonds = [{df_bonds.columns[i]: df_bonds[df_bonds.columns[i]][j] for i in range(len(df_bonds.columns))} for j in range(len(df_bonds))]
list_gold_bonds = [{df_gold_bonds.columns[i]: df_gold_bonds[df_gold_bonds.columns[i]][j] for i in range(len(df_gold_bonds.columns))} for j in range(len(df_gold_bonds))]

collection_mutual_funds.insert_many(list_mfs)
collection_etfs.insert_many(list_etfs)
collection_crypto.insert_many(list_cryptos)
collection_bonds.insert_many(list_bonds)
collection_gold_bonds.insert_many(list_gold_bonds)