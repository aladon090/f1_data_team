from Extract import Extract  # your API Extract class
from Transform import Transform
# Extract JSON from API
extractor = Extract()
team_json = extractor.extract()

# Transform JSON to clean DataFrame
transformer = Transform()
team_df = transformer.transform(team_json)

print(team_df.head())
