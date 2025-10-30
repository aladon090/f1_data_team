from Extract import Extract
from Transform import Transform
from Load import Load

# Extract
extractor = Extract()
team_json = extractor.extract()

# Transform
transformer = Transform()
df = transformer.clean_rows(team_json)
clean_df = transformer.clean_unknowns(df)

# Load
loader = Load()
loader.create_sql_table()
loader.load_sql_table(clean_df)
