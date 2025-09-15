#Conexion
#region
import pandas as pd  #pip install pandas
import numpy as np
import pyodbc as odbc #pip install pyodbc
import urllib
from sqlalchemy import create_engine #pip install SQLAlchemy
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet,ParagraphStyle

# Arma tu cadena de conexión ODBC
connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=p-;"
    "DATABASE=;"
    "UID=;"
    "PWD=;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

# Se codifica (URL-encode) la cadena para que sea compatible con SQLAlchemy
params = urllib.parse.quote_plus(connection_string)

# Creamos la URI (o DSN) de conexión para SQLAlchemy
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
#endregion



#--------------------------------------NEW ONBOARDED----------------------------------------
#region
query_datahistory = "select AccountUuid, Status, UpdatedAt from Data_WalletHistory"
df_datahistory_raw = pd.read_sql(query_datahistory, engine)
df_datahistory = df_datahistory_raw.sort_values(by="UpdatedAt", ascending=False)
df_datahistory = df_datahistory.drop_duplicates(subset=['AccountUuid', 'Status', 'UpdatedAt'])

df_datahistory.loc[(df_datahistory['Status'] == 'PROCESSING') , 'Amount'] = -1
df_datahistory.loc[(df_datahistory['Status'] == 'ACTIVE') , 'Amount'] = -3.5
df_datahistory['YMD'] = df_datahistory['UpdatedAt'].dt.to_period('D')  
df_datahistory = df_datahistory.drop(columns=["UpdatedAt"])
df_datahistory.rename(columns={'Status': 'Type'}, inplace=True)

df_datahistory_2024 = df_datahistory[df_datahistory['YMD'] < '2025-01-01'].sort_values(by="YMD", ascending=True)
query_account = "Select  ACCOUNTUUID,RO_PROCESSING,FB_PROCESSING,FB_ACTIVE,RO_ACTIVE from ConsumerStatusHistory"
df_acc_raw = pd.read_sql(query_account, engine)

df_acc = df_acc_raw
df_newonboarded_raw = df_acc_raw

df_newonboarded_raw[['RO_PROCESSING', 'RO_ACTIVE', 'FB_PROCESSING', 'FB_ACTIVE']] = df_newonboarded_raw[['RO_PROCESSING', 'RO_ACTIVE', 'FB_PROCESSING', 'FB_ACTIVE']].apply(pd.to_datetime)
# Columnas a transformar en filas
value_vars = ['RO_PROCESSING', 'RO_ACTIVE', 'FB_PROCESSING', 'FB_ACTIVE']  # Puedes agregar más si es necesario

# Transformar la tabla
df_newonboarded_raw = df_newonboarded_raw.melt(
    id_vars=['ACCOUNTUUID'],  # Mantener la columna AccountUuid
    value_vars=value_vars,  # Columnas a convertir en filas
    var_name='Type',  # Nombre de la nueva columna que indica el tipo de proceso
    value_name='Date'  # Nombre de la nueva columna que contendrá los valores de fecha
)
df_newonboarded_raw = df_newonboarded_raw.dropna(subset=['Date'])


df_newonboarded = df_newonboarded_raw[~df_newonboarded_raw["Type"].isin(['FB_PROCESSING','RO_PROCESSING'])]
df_newonboarded['YMD'] = df_newonboarded['Date'].dt.to_period('D') 
df_newonboarded = df_newonboarded[df_newonboarded['Date'] >= '2025-01-01']
df_newonboarded = df_newonboarded.drop(['Type', 'Date'], axis=1)
df_newonboarded.rename(columns={'ACCOUNTUUID': 'AccountUuid'}, inplace=True)


df_datahistory_onboarded_raw = df_datahistory_2024.copy()
df_datahistory_onboarded = df_datahistory_onboarded_raw[~df_datahistory_onboarded_raw["Type"].isin(['PROCESSING'])]
df_datahistory_onboarded = df_datahistory_onboarded.drop(['Type', 'Amount'], axis=1)

df_newonboarded['Flag'] = df_newonboarded['AccountUuid'].isin(df_datahistory_onboarded['AccountUuid']).astype(int)
df_newonboarded = df_newonboarded[df_newonboarded['Flag'] == 0]
df_newonboarded = df_newonboarded.drop(['Flag'], axis=1)
df_newonboarded_merge = pd.concat([df_newonboarded, df_datahistory_onboarded], ignore_index=True)

#endregion


#--------------------------------------NEW CREATED----------------------------------------
#region
query_datahistory_new = "select AccountUuid, Status, UpdatedAt from Data_WalletHistoryProspect WHERE Status = 'NEW'"
df_datahistory_new_raw = pd.read_sql(query_datahistory_new, engine)
df_datahistory_new = df_datahistory_new_raw.sort_values(by="UpdatedAt", ascending=False)
df_datahistory_new['Creation_date'] = df_datahistory_new['UpdatedAt'].dt.to_period('D') 
df_datahistory_new = df_datahistory_new.drop(['Status', 'UpdatedAt'], axis=1)
df_datahistory_new["Status"] = "Created"


#endregion


#--------------------------------------PLAID----------------------------------------
#region
query = "Select  distinct(AccountUuid) from Data_BankPaymentMethod WHERE  Status = 'ACTIVE' "
df_raw_plaid = pd.read_sql(query, engine)



df_p = df_raw_plaid
df_p['Plaid'] = 1
#endregion


#--------------------------------------ISSUED CARDS----------------------------------------
#region
query_cards = "Select  distinct(AccountUuid) from Data_IssuedCardsLastStatus"
df_raw_issued_cards = pd.read_sql(query_cards, engine)

df_raw_issued_cards['Issued_Card'] = 1
#endregion


#--------------------------------------PYMT METHOD----------------------------------------
#region
query_pmethod = "Select  distinct(AccountUuid) from CardPaymentMethod WHERE  Status = 'ACTIVE'"
df_raw_p_method = pd.read_sql(query_pmethod, engine)

df_raw_p_method['Pmt_method'] = 1
#endregion


#--------------------------------------TRANSACTING----------------------------------------
#region
query_alviere = "Select  distinct(AccountUuid) from Data_Alviere"

df_alviere_raw = pd.read_sql(query_alviere, engine)
df_alviere_raw['Transacting'] = 1
#endregion


#--------------------------------------CONSUMER REVIEW----------------------------------------
#region
query_consumer_review = "Select account_uuid, YearOfBirth, HaveSSN, postal_code from Data_ConsumerReview Where account_uuid is not null"
df_consumer_raw = pd.read_sql(query_consumer_review, engine)

ruta_csv = r"C:\Users\ChristianGarza\Desktop\Map\uszips.csv"
df_zip = pd.read_csv(ruta_csv)
df_zip.columns
df_zip = df_zip.drop(['zcta',
        'parent_zcta', 'population', 'county_fips', 'county_name',
        'county_weights', 'county_names_all', 'county_fips_all', 'imprecise',
        'military', 'timezone'], axis=1)

df_consumer_raw['postal_code'] = df_consumer_raw['postal_code'].astype(str)
df_zip['zip'] = df_zip['zip'].astype(str)

# Lista de columnas que quieres agregar desde df_zip
columnas_a_unir = ['zip', 'lat', 'lng', 'city', 'state_id', 'state_name', 'density']

# Hacer el left join
df_consumer_merged = pd.merge(
    df_consumer_raw,
    df_zip[columnas_a_unir],
    how='left',
    left_on='postal_code',
    right_on='zip'
)
df_consumer_merged = df_consumer_merged.drop(['zip'], axis=1)
#endregion




#--------------------------------------JOIN----------------------------------------
#region

df_final_merged = pd.merge(
    df_newonboarded_merge,
    df_datahistory_new[['AccountUuid','Creation_date']],
    how='left',
    left_on='AccountUuid',
    right_on='AccountUuid'
)

df_final_merged = pd.merge(
    df_final_merged,
    df_p[['AccountUuid','Plaid']],
    how='left',
    left_on='AccountUuid',
    right_on='AccountUuid'
)

df_final_merged = pd.merge(
    df_final_merged,
    df_raw_issued_cards[['AccountUuid','Issued_Card']],
    how='left',
    left_on='AccountUuid',
    right_on='AccountUuid'
)

df_final_merged = pd.merge(
    df_final_merged,
    df_alviere_raw[['AccountUuid','Transacting']],
    how='left',
    left_on='AccountUuid',
    right_on='AccountUuid'
)

df_final_merged = pd.merge(
    df_final_merged,
    df_raw_p_method[['AccountUuid','Pmt_method']],
    how='left',
    left_on='AccountUuid',
    right_on='AccountUuid'
)

df_final_merged = pd.merge(
    df_final_merged,
    df_consumer_merged[['account_uuid', 'YearOfBirth', 'HaveSSN', 'postal_code', 'lat', 'lng','city',  'state_name', 'density']],
    how='left',
    left_on='AccountUuid',
    right_on='account_uuid'
)
df_final_merged = df_final_merged.drop(['account_uuid'], axis=1)

df_final_merged.columns
df_final_merged["density"].unique()
df_final_merged["density"].isna().sum()
df_final_merged["YearOfBirth"].isna().value_counts()
df_final_merged[["Plaid","Issued_Card",'Pmt_method',"Transacting"]] = df_final_merged[["Plaid","Issued_Card",'Pmt_method',"Transacting"]].fillna(0)
df_final_merged = df_final_merged.dropna(subset=["postal_code"])
df_final_merged = df_final_merged.dropna(subset=["state_name"])

df_final_merged.isna().sum()
(df_final_merged.isna().sum() / len(df_final_merged) * 100).round(2)
#endregion

df_final_merged.to_csv("user_clasification.csv", index=False)

