# scheduled-reports
Scheduled tasks on Azure. The five reports were placed on GCP automately. 

Five subtasks in it.    
1. Compare the data between MongoDB_account with CRM_accounts
2. Compare the data between MongoDB_accountusers with CRM_contracts
3. Compare the data between MongoDB_listings with CRM_orgs
4. Compare the data between SQLServer with CRM_users
5. Compare the data between CRM email with MongoDB email.



![image](https://user-images.githubusercontent.com/75282285/184558838-04f7ac5f-0f40-4930-a9e9-7b1f1b8fc65a.png)



![image](https://user-images.githubusercontent.com/75282285/184558852-d041f219-a69d-4096-8c85-b69b7cfc4a62.png)

# The libraries used:
functions_elasticsearch.py
```python
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, MultiSearch
```

functions_mongo.py and queries.py
~~~
from pymongo import MongoClient
~~~

functions_zoho.py
```
base_url = constants.ZOHO_URL_TOKEN
params = settings.CRM_ZOHO
```
