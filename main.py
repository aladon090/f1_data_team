from Extract import Extract
from Transform import Transform
import pandas as pd

extractor = Extract()
team_json = extractor.extract()

transformer = Transform()
df = transformer.clean_rows(data_team=team_json)
clean_df = transformer.clean_unknowns(df)

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows

print(clean_df)


