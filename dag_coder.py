
# In[4]:
import pandas as pd
import requests
from functools import reduce


import sqlalchemy as sa
import config
# DB conecton


def proceso_etl():
    """
    Solución temporal para carga de datos FMI a tabla Redshift
    
    """

    user = 'marianoradusky_19_coderhouse'

    #conn_string = f"host={config.host} dbname={config.db} user={user} password={config.pwd} port=5439 options='-csearch_path={dbschema}'"

    #conn = psycopg2.connect(conn_string)

    # El "engine" es nuestra puerta de entrada a la DB
    conn = sa.create_engine(
        f"postgresql://{user}:{config.pwd}@{config.host}:5439/{config.db}",
        connect_args={'options': f'-c search_path={user}'})


    # In[73]:


    conn.execute("""
        CREATE TABLE IF NOT EXISTS imf_monetary_data (
            id INT IDENTITY(1,1) PRIMARY KEY,
            date TIMESTAMP,
            country VARCHAR,
            broad_money FLOAT,
            currency_in_circulation FLOAT,
            monetary_base FLOAT,
            entity VARCHAR UNIQUE
        )
        SORTKEY (date, country);
    """)
    print('corro la creacion de tabla')
    # In[9]:




    # In[10]:


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



    # In[11]:


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


    # In[12]:


    # Función que trea el listado de países del FMI

    diccionario_paises = get_counries()


    # In[13]:


    # Función para obtener la data de las variables solicitadas en el ISSUE

    df1 = ifm_data(series_dict1, "M", diccionario_paises)
    df2 = ifm_data(series_dict2, "M", diccionario_paises)
    df3 = ifm_data(series_dict3, "M", diccionario_paises)


    # In[14]:


    data = pd.concat([df1,df2,df3])


    # In[15]:


    # Paso a float las columnas numericas
    for col in data.columns:
        try:
            data[col] = data[col].astype(float)
        except:
            continue


    # In[16]:


    data = data.reset_index()
    data.columns = [x.replace(' ','_').lower() for x in data.columns]


    # In[35]:


    # genero dos condiciones, str date y una columna unica para garantizar unicidad de los datos
    data['date'] = data['date'].dt.strftime('%Y-%m-%d')


    # In[41]:


    data['entity'] = data['date'] + '-' + data['country']


    # In[70]:


    data = data.fillna(0)


    # In[71]:


    #genero los chunks para insertar los datos de a 100, lo hago asi ya que sino quedaba muy pesado el upload
    chunk_size = 100
    chunks = [','.join([str(tuple(row)) for row in data[i:i + chunk_size].values]) for i in range(0, len(data), chunk_size)]


    # In[75]:

    print('creo la tabla subir')
    for i in chunks:
        conn.execute(f"""
        insert into imf_monetary_data (date, broad_money, country, currency_in_circulation, monetary_base, entity) values {i}""")
    print('subio toda la data')

    # In[ ]:


# my_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 9, 11),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval here
    catchup=False  # Set to False to skip running backfill
)


run_my_function_task = PythonOperator(
    task_id='run_my_function',
    python_callable=proceso_etl,
    dag=dag,
)
