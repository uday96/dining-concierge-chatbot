# Assignment 1: Dining Concierge AI Agent

# Contributors

- Nandini Agrawal na2928
- Uday Theja um2162

## Files

- **dining-concierge-chatbot-web/**: The frontend related files which were uploaded to S3 bucket
- **dining-suggestions-DRAFT-6DPSVVG7VY-LexJson/**: The Lex bot export
- **lambda-functions/** : The files corresponding to LF0, LF1 and LF2                           
- **dynamodb_to_es.py**: Script to convert dynamodb data into elastic search format                           
- **scrape_yelp.py**: Script to scrape restaurant data from Yelp
- **es_data.json**: Output of dynamodb_to_es.py which is pushed to Elasticsearch using *"curl -XPOST -u 'master-user:master-pwd' 'https://<es_endpoint>/_bulk' --data-binary @es_data.json -H 'Content-Type: application/json'"*                          
- **dynamodb_data.json**: The export of dynamo DB "yelp-restaurants" table                
- **yelp-data.json**: The output of "scrape_yelp.py" script