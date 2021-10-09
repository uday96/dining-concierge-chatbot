import requests
import json

cuisines = ["chinese restaurants","indian restaurants","italian restaurants","mexican restaurants","thai restaurants","japanese restaurants","korean restaurants"]
my_headers = {"Authorization" : "Bearer {token}"}
all_restuarants=[]
for i in range(len(cuisines)):
    for j in range(0,200,50):
        query = {"location":"Manhattan","term":cuisines[i],"limit":50,"offset":j}
        response = requests.get("https://api.yelp.com/v3/businesses/search?", params=query,headers=my_headers)
        b = response.json()
        print(cuisines[i],b["total"])
        for body in b["businesses"]:
            required = {
                "restaurant-id" : body["id"],
                "restaurant-name": body["name"],
                "restaurant-cuisine": cuisines[i],
                "restaurant-url":body["url"],
                "restaurant-review_count":body["review_count"],
                "restaurant-rating":body["rating"],
                "restaurant-coordinates":body["coordinates"],
                "restaurant-location":body["location"],
                "restaurant-display_phone":body["display_phone"],
            }
            all_restuarants.append(required)

with open('yelp-data.json', 'w') as f:
    json.dump(all_restuarants, f)