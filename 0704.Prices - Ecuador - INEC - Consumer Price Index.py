#!/usr/bin/env python
# coding: utf-8

# In[5]:


import requests
from bs4 import BeautifulSoup
import io
from zipfile import ZipFile
import pandas as pd

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("ALPHACAST_API_KEY")
alphacast = Alphacast(API_KEY)


page = requests.get('https://www.ecuadorencifras.gob.ec/indice-de-precios-al-consumidor/')
soup = BeautifulSoup(page.content, 'html.parser')

#En la pagina hay formato csv y formato Excel. El problema con el ultimo es que es mucho más pesado que el CSV
file_zip = []
for link in soup.find_all('a'):
    if 'zip' in link.get('href') and 'CSV' in link.get('href'):
        file_zip.append(link.get('href'))


#Descargo el archivo y lo guardo como un zip
response = requests.get(file_zip[0], stream=True)
zip_file = ZipFile(io.BytesIO(response.content))


#Genero un listado con los nombres de los archivos dentro del zip
names = zip_file.namelist()
#Determino el nombre del archivo zip que tiene este formato SERIE HISTORICA IPC_mes_ano.zip
name_file_zip = [string for string in names if 'Tabulados y series historicas_CSV/SERIE HISTORICA' in string][0]

#Abro el zip dentro del zip
name_file_zip2 = zip_file.open(name_file_zip)
#Lo trato como un archivo Zip para poder 
zip_file2 = ZipFile(name_file_zip2)
#Se extrae el nombre del archivo que corresponde al IPC historico
name_file_csv = zip_file2.namelist()[1]

#Se lee el csv
df = pd.read_csv(zip_file2.open(name_file_csv), encoding = "ISO-8859-1", skiprows=2, header=[1])

#Se eliminan columnas vacias y filas vacias
df.dropna(how='all', axis=1, inplace=True)
df.dropna(how='all', axis=0, inplace=True)

#Para eliminar las filas adicionales con Notas, se eliminan NaN de la segunda columna (Enero)
df.dropna(subset=[df.columns[1]], inplace=True)

#Hago un stack y reseteo el indice
df = df.set_index(df.columns[0]).stack().reset_index()

#Reemplazo los nombres de los meses por el numero
dict_meses = {'Enero':'01', 'Febrero':'02', 'Marzo':'03', 'Abril':'04', 'Mayo':'05', 'Junio':'06', 'Julio':'07',
       'Agosto':'08', 'Septiembre':'09', 'Octubre':'10', 'Noviembre':'11', 'Diciembre':'12'}

df['level_1'] = df['level_1'].replace(dict_meses)

df['Date'] = pd.to_datetime(df['MESES'] + '-' + df['level_1'], format='%Y-%m', errors='coerce')

df.drop(['MESES', 'level_1'], axis=1, inplace=True)

df.rename(columns={0:'Nivel general - Indice de precios'}, inplace=True)
df.set_index('Date', inplace=True)
df['country'] = 'Ecuador'

#Cargo la data a Alphacast
alphacast.datasets.dataset(7695).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)