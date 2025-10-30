from Extract import Extract


extractor = Extract()
df_api = extractor.extract(URL='https://v1.formula-1.api-sports.io/teams')
print(df_api.head())

