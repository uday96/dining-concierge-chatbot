import json

DYANMODB_FILE = "dynamodb_data.json"
ELASTICSEARCH_FILE = "es_data.json"
ELASTICSEARCH_INDEX = "restaurants"

with open(DYANMODB_FILE, "r") as f:
    db_data = json.load(f)

with open(ELASTICSEARCH_FILE, "w") as f:
    for item in db_data['Items']:
        idx = item['restaurant-id']['S']
        cuisine = item['restaurant-cuisine']['S']
        f.write(json.dumps({"index": {"_index": ELASTICSEARCH_INDEX, "_id": idx}}))
        f.write("\n")
        f.write(json.dumps({"restaurant-id": idx, "restaurant-cuisine": cuisine}))
        f.write("\n")