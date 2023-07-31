#!/usr/bin/env python
# coding: utf-8

# In[1]:


# !pip install sqlalchemy library needed


# In[2]:


# !pip install psycopg2-binary library needed


# In[3]:


# library needed
# !pip install "redshift_connector[full]"
# !pip3 install sqlalchemy-redshift


# In[4]:


import sqlalchemy as sa
import config
# DB conecton
user = 'marianoradusky_19_coderhouse'

#conn_string = f"host={config.host} dbname={config.db} user={user} password={config.pwd} port=5439 options='-csearch_path={dbschema}'"

#conn = psycopg2.connect(conn_string)

# El "engine" es nuestra puerta de entrada a la DB
conn = sa.create_engine(
    f"postgresql://{user}:{config.pwd}@{config.host}:5439/{config.db}",
    connect_args={'options': f'-c search_path={user}'})


# In[5]:


# conn.execute("""
#         CREATE TABLE imf_monetary_data (
#             date TIMESTAMP,
#             country VARCHAR,
#             broad_money FLOAT,
#             currency_in_circulation FLOAT,
#             monetary_base FLOAT,
#         );
#              """)


# In[6]:


import pandas as pd
import requests
from functools import reduce


# In[7]:


#Conexión a datos de fmi por api pública de descarga libre
def get_counries():    
    url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
    key = 'Dataflow'
    series = 'IFS'

    #Sabiendo la fuente, se incorpora el código a la key y se buscan las dimensiones
    key = 'DataStructure/{}'.format(series)

    dimension_list = requests.get(f'{url}{key}').json()['Structure']['KeyFamilies']['KeyFamily']['Components']['Dimension']

    dimension_dict = {}

    for i in range(0, len(dimension_list)):
        dimension_dict['Dimension {}'.format(i+1)] = dimension_list[i]['@codelist']

    #Sabiendo las dimensiones, se obtienen los países y los indicadores disponibles
    code_dimension_dict = {}

    for i in range(0, len(dimension_dict)):
        key = 'CodeList/{}'.format(dimension_dict[list(dimension_dict.keys())[i]])

        try:
            aux = requests.get('{}{}'.format(url,key)).json()['Structure']['CodeLists']['CodeList']['Code']

            aux_dict = {}

            for x in range(0, len(aux)):
                aux_value = aux[x]['@value']
                aux_text = aux[x]['Description']['#text']
                aux_dict[aux_value] = aux_text

                code_dimension_dict[dimension_dict[list(dimension_dict.keys())[i]]] = aux_dict

        except:
            pass

    #Diccionario que recopila los códigos de países
    diccionario_paises = code_dimension_dict[list(code_dimension_dict.keys())[0]]
    return diccionario_paises




def ifm_data(variables, frecuency, diccionario_paises):
    url = f'http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/{frecuency}..'
    for serie in variables:        
        url = url + serie + "+" 
    url = url[:-1]    
    r = requests.get(url)
    json = r.json()
    df_base = pd.json_normalize(json["CompactData"]["DataSet"]["Series"])
    series = df_base["@INDICATOR"].unique().tolist()
    dfs = []
    for name in series:        
        df = df_base[df_base["@INDICATOR"] == name]    
        dfs.append(df)
    df_final = pd.DataFrame()
    for df in dfs:
        dataframe = df[~df["Obs"].isnull()]
        countries = dataframe["@REF_AREA"].to_list()
        data = dataframe["Obs"].to_list()
        indicator = dataframe["@INDICATOR"].values[0]
        for i, d in enumerate(data):    
            df = pd.json_normalize(d)
            df["Country"] = countries[i]
            df["Serie"] = indicator
            #df = df.set_index("@TIME_PERIOD")
            df_final = pd.concat([df_final, df])
    try:
        df_final = df_final.drop(["@OBS_STATUS"], axis=1)
    except:
        pass
    finals_dfs = []    
    for serie in series:
        df = df_final[df_final["Serie"]==serie]
        df = df.rename(columns={"@OBS_VALUE":variables[serie]})
        df = df.iloc[:,:-1]        
        finals_dfs.append(df)
        
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=['@TIME_PERIOD', "Country"], how="outer"), finals_dfs)
    merged_df = merged_df.set_index("@TIME_PERIOD")
    merged_df.index.name = "Date"
    merged_df.index = merged_df.index.map(pd.to_datetime)
    #merged_df["Country"] = merged_df["Country"].apply(lambda x: x[0:2])
    merged_df["Country"] = merged_df["Country"].map(diccionario_paises)
    #merged_df = merged_df.dropna(subset=["Country"])
    return merged_df



# In[8]:


# Series a pasar a la funcion, según ISSUE: 56102

series_dict1 = {
    "FASMB_USD":"Monetary Base",
    "FASMBC_USD":"Currency in Circulation",
    "FMB_USD":"Broad Money",
}

series_dict2 = {
    "FASMB_EUR":"Monetary Base",
    "FASMBC_EUR":"Currency in Circulation",
    "FMB_EUR":"Broad Money",
}

series_dict3 = {
    "FASMB_XDC":"Monetary Base",
    "FASMBC_XDC":"Currency in Circulation",
    "FMB_XDC":"Broad Money",
}


# In[9]:


# Función que trea el listado de países del FMI

diccionario_paises = get_counries()


# In[10]:


# Función para obtener la data de las variables solicitadas en el ISSUE

df1 = ifm_data(series_dict1, "M", diccionario_paises)
df2 = ifm_data(series_dict2, "M", diccionario_paises)
df3 = ifm_data(series_dict3, "M", diccionario_paises)


# In[11]:


data = pd.concat([df1,df2,df3])


# In[12]:


# Paso a float las columnas numericas
for col in data.columns:
    try:
        data[col] = data[col].astype(float)
    except:
        continue


# In[13]:


data = data.reset_index()
data.columns = [x.replace(' ','_').lower() for x in data.columns]


# In[15]:


#Tuve que subir 5 porque el total (38000 filas no lo procesaba)
data.head().to_sql(f'imf_monetary_data',
                conn,
               index=False, if_exists='replace')


# In[ ]:



