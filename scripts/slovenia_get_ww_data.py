import pandas as pd

df = pd.read_csv('https://podatki.gov.si/dataset/1b72495b-a13c-4c5f-9c3c-c99c83570998/resource/5f546967-f6e8-4807-a928-4c5f2a3f8e59/download/ocenjenostokuzenihosebssarscov2.csv')

df.to_csv('data/Slovenia/sl_wastewater_data.csv')
