```python
# Importing Eland
# Python Client and Toolkit for DataFrames, Big Data, Machine Learning and ETL in Elasticsearch
# https://eland.readthedocs.io/en/latest/
# https://github.com/elastic/eland
import eland as ed

# https://elasticsearch-dsl.readthedocs.io/en/latest/
# https://github.com/elastic/elasticsearch-dsl-py
# High level Python client for Elasticsearch
from elasticsearch_dsl import Search, Q

# https://elasticsearch-py.readthedocs.io/en/latest/
# https://github.com/elastic/elasticsearch-py
# Official Python low-level client for Elasticsearch
from elasticsearch import Elasticsearch

# Import pandas and numpy for data wrangling
import pandas as pd
import numpy as np

# For pretty-printing
# Function for pretty-printing JSON
def json(raw):
    import json
    print(json.dumps(raw, indent=2, sort_keys=True))
```


```python
# Connect to an Elasticsearch instance
# here we use the official Elastic Python client
# check it on https://github.com/elastic/elasticsearch-py
es = Elasticsearch(
  ['http://localhost:9200'],
  http_auth=("es_kbn", "changeme")
)
# print the connection object info (same as visiting http://localhost:9200)
# make sure your elasticsearch node/cluster respond to requests
json(es.info())
```

    {
      "cluster_name": "churn",
      "cluster_uuid": "K3nB4fp_QcyjpY-e2XVUbA",
      "name": "node-01",
      "tagline": "You Know, for Search",
      "version": {
        "build_date": "2020-07-22T19:31:37.655268Z",
        "build_flavor": "default",
        "build_hash": "bbbd2282a6668869c41efc5713ad8214d44c0ad1",
        "build_snapshot": true,
        "build_type": "zip",
        "lucene_version": "8.6.0",
        "minimum_index_compatibility_version": "7.0.0",
        "minimum_wire_compatibility_version": "7.10.0",
        "number": "8.0.0-SNAPSHOT"
      }
    }
    


```python
# name of the index we want to query
index_name = 'kibana_sample_data_ecommerce' 

# defining the search statement to get all records in an index
search = Search(using=es, index=index_name).query("match_all") 

# retrieving the documents from the search
documents = [hit.to_dict() for hit in search.scan()] 

# converting the list of hit dictionaries into a pandas dataframe:
df_ecommerce = pd.DataFrame.from_records(documents)

```


```python
# visualizing the dataframe with the results:
df_ecommerce.head()['geoip']
```




    0    {'country_iso_code': 'EG', 'location': {'lon':...
    1    {'country_iso_code': 'AE', 'location': {'lon':...
    2    {'country_iso_code': 'US', 'location': {'lon':...
    3    {'country_iso_code': 'GB', 'location': {'lon':...
    4    {'country_iso_code': 'EG', 'location': {'lon':...
    Name: geoip, dtype: object




```python
# retrieving a summary of the columns in the dataset:
df_ecommerce.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 4675 entries, 0 to 4674
    Data columns (total 24 columns):
     #   Column                 Non-Null Count  Dtype  
    ---  ------                 --------------  -----  
     0   category               4675 non-null   object 
     1   currency               4675 non-null   object 
     2   customer_first_name    4675 non-null   object 
     3   customer_full_name     4675 non-null   object 
     4   customer_gender        4675 non-null   object 
     5   customer_id            4675 non-null   int64  
     6   customer_last_name     4675 non-null   object 
     7   customer_phone         4675 non-null   object 
     8   day_of_week            4675 non-null   object 
     9   day_of_week_i          4675 non-null   int64  
     10  email                  4675 non-null   object 
     11  manufacturer           4675 non-null   object 
     12  order_date             4675 non-null   object 
     13  order_id               4675 non-null   int64  
     14  products               4675 non-null   object 
     15  sku                    4675 non-null   object 
     16  taxful_total_price     4675 non-null   float64
     17  taxless_total_price    4675 non-null   float64
     18  total_quantity         4675 non-null   int64  
     19  total_unique_products  4675 non-null   int64  
     20  type                   4675 non-null   object 
     21  user                   4675 non-null   object 
     22  geoip                  4675 non-null   object 
     23  event                  4675 non-null   object 
    dtypes: float64(2), int64(5), object(17)
    memory usage: 876.7+ KB
    


```python
# getting descriptive statistics from the dataframe
df_ecommerce.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>day_of_week_i</th>
      <th>order_id</th>
      <th>taxful_total_price</th>
      <th>taxless_total_price</th>
      <th>total_quantity</th>
      <th>total_unique_products</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>4675.000000</td>
      <td>4675.000000</td>
      <td>4675.000000</td>
      <td>4675.000000</td>
      <td>4675.000000</td>
      <td>4675.000000</td>
      <td>4675.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>27.465668</td>
      <td>3.114866</td>
      <td>587280.693690</td>
      <td>75.050062</td>
      <td>75.050062</td>
      <td>2.158503</td>
      <td>2.157647</td>
    </tr>
    <tr>
      <th>std</th>
      <td>13.946711</td>
      <td>1.930395</td>
      <td>47891.081603</td>
      <td>52.793929</td>
      <td>52.793929</td>
      <td>0.594071</td>
      <td>0.588510</td>
    </tr>
    <tr>
      <th>min</th>
      <td>4.000000</td>
      <td>0.000000</td>
      <td>550375.000000</td>
      <td>6.990000</td>
      <td>6.990000</td>
      <td>1.000000</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>17.000000</td>
      <td>1.000000</td>
      <td>562044.000000</td>
      <td>44.980000</td>
      <td>44.980000</td>
      <td>2.000000</td>
      <td>2.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>27.000000</td>
      <td>3.000000</td>
      <td>573671.000000</td>
      <td>63.980000</td>
      <td>63.980000</td>
      <td>2.000000</td>
      <td>2.000000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>41.000000</td>
      <td>5.000000</td>
      <td>585415.000000</td>
      <td>92.980000</td>
      <td>92.980000</td>
      <td>2.000000</td>
      <td>2.000000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>52.000000</td>
      <td>6.000000</td>
      <td>740002.000000</td>
      <td>2249.920000</td>
      <td>2249.920000</td>
      <td>8.000000</td>
      <td>4.000000</td>
    </tr>
  </tbody>
</table>
</div>




```python
# loading the data from the Sample Ecommerce data from Kibana into Eland dataframe:
ed_ecommerce = ed.read_es(es, index_name)
# visualizing the results:
ed_ecommerce.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>currency</th>
      <th>customer_birth_date</th>
      <th>customer_first_name</th>
      <th>customer_full_name</th>
      <th>customer_gender</th>
      <th>customer_id</th>
      <th>customer_last_name</th>
      <th>customer_phone</th>
      <th>day_of_week</th>
      <th>...</th>
      <th>products.taxful_price</th>
      <th>products.taxless_price</th>
      <th>products.unit_discount_amount</th>
      <th>sku</th>
      <th>taxful_total_price</th>
      <th>taxless_total_price</th>
      <th>total_quantity</th>
      <th>total_unique_products</th>
      <th>type</th>
      <th>user</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Fy_X0nMBxx5L21Ced97s</th>
      <td>[Men's Clothing]</td>
      <td>EUR</td>
      <td>NaT</td>
      <td>Eddie</td>
      <td>Eddie Underwood</td>
      <td>MALE</td>
      <td>38</td>
      <td>Underwood</td>
      <td></td>
      <td>Monday</td>
      <td>...</td>
      <td>[11.99, 24.99]</td>
      <td>[11.99, 24.99]</td>
      <td>[0, 0]</td>
      <td>[ZO0549605496, ZO0299602996]</td>
      <td>36.98</td>
      <td>36.98</td>
      <td>2</td>
      <td>2</td>
      <td>order</td>
      <td>eddie</td>
    </tr>
    <tr>
      <th>GC_X0nMBxx5L21Ced97t</th>
      <td>[Women's Clothing]</td>
      <td>EUR</td>
      <td>NaT</td>
      <td>Mary</td>
      <td>Mary Bailey</td>
      <td>FEMALE</td>
      <td>20</td>
      <td>Bailey</td>
      <td></td>
      <td>Sunday</td>
      <td>...</td>
      <td>[24.99, 28.99]</td>
      <td>[24.99, 28.99]</td>
      <td>[0, 0]</td>
      <td>[ZO0489604896, ZO0185501855]</td>
      <td>53.98</td>
      <td>53.98</td>
      <td>2</td>
      <td>2</td>
      <td>order</td>
      <td>mary</td>
    </tr>
    <tr>
      <th>GS_X0nMBxx5L21Ced97t</th>
      <td>[Women's Shoes, Women's Clothing]</td>
      <td>EUR</td>
      <td>NaT</td>
      <td>Gwen</td>
      <td>Gwen Butler</td>
      <td>FEMALE</td>
      <td>26</td>
      <td>Butler</td>
      <td></td>
      <td>Sunday</td>
      <td>...</td>
      <td>[99.99, 99.99]</td>
      <td>[99.99, 99.99]</td>
      <td>[0, 0]</td>
      <td>[ZO0374603746, ZO0272202722]</td>
      <td>199.98</td>
      <td>199.98</td>
      <td>2</td>
      <td>2</td>
      <td>order</td>
      <td>gwen</td>
    </tr>
    <tr>
      <th>Gi_X0nMBxx5L21Ced97t</th>
      <td>[Women's Shoes, Women's Clothing]</td>
      <td>EUR</td>
      <td>NaT</td>
      <td>Diane</td>
      <td>Diane Chandler</td>
      <td>FEMALE</td>
      <td>22</td>
      <td>Chandler</td>
      <td></td>
      <td>Sunday</td>
      <td>...</td>
      <td>[74.99, 99.99]</td>
      <td>[74.99, 99.99]</td>
      <td>[0, 0]</td>
      <td>[ZO0360303603, ZO0272002720]</td>
      <td>174.98</td>
      <td>174.98</td>
      <td>2</td>
      <td>2</td>
      <td>order</td>
      <td>diane</td>
    </tr>
    <tr>
      <th>Gy_X0nMBxx5L21Ced97t</th>
      <td>[Men's Clothing, Men's Accessories]</td>
      <td>EUR</td>
      <td>NaT</td>
      <td>Eddie</td>
      <td>Eddie Weber</td>
      <td>MALE</td>
      <td>38</td>
      <td>Weber</td>
      <td></td>
      <td>Monday</td>
      <td>...</td>
      <td>[59.99, 20.99]</td>
      <td>[59.99, 20.99]</td>
      <td>[0, 0]</td>
      <td>[ZO0542505425, ZO0601306013]</td>
      <td>80.98</td>
      <td>80.98</td>
      <td>2</td>
      <td>2</td>
      <td>order</td>
      <td>eddie</td>
    </tr>
  </tbody>
</table>
</div>
<p>5 rows × 46 columns</p>




```python
# retrieving a summary of the columns in the dataframe:
ed_ecommerce.info()
```

    <class 'eland.dataframe.DataFrame'>
    Index: 4675 entries, Fy_X0nMBxx5L21Ced97s to XC_X0nMBxx5L21CejPB3
    Data columns (total 46 columns):
     #   Column                         Non-Null Count  Dtype         
    ---  ------                         --------------  -----         
     0   category                       4675 non-null   object        
     1   currency                       4675 non-null   object        
     2   customer_birth_date            0 non-null      datetime64[ns]
     3   customer_first_name            4675 non-null   object        
     4   customer_full_name             4675 non-null   object        
     5   customer_gender                4675 non-null   object        
     6   customer_id                    4675 non-null   object        
     7   customer_last_name             4675 non-null   object        
     8   customer_phone                 4675 non-null   object        
     9   day_of_week                    4675 non-null   object        
     10  day_of_week_i                  4675 non-null   int64         
     11  email                          4675 non-null   object        
     12  event.dataset                  4675 non-null   object        
     13  geoip.city_name                4094 non-null   object        
     14  geoip.continent_name           4675 non-null   object        
     15  geoip.country_iso_code         4675 non-null   object        
     16  geoip.location                 4675 non-null   object        
     17  geoip.region_name              3924 non-null   object        
     18  manufacturer                   4675 non-null   object        
     19  order_date                     4675 non-null   datetime64[ns]
     20  order_id                       4675 non-null   object        
     21  products._id                   4675 non-null   object        
     22  products.base_price            4675 non-null   float64       
     23  products.base_unit_price       4675 non-null   float64       
     24  products.category              4675 non-null   object        
     25  products.created_on            4675 non-null   datetime64[ns]
     26  products.discount_amount       4675 non-null   float64       
     27  products.discount_percentage   4675 non-null   float64       
     28  products.manufacturer          4675 non-null   object        
     29  products.min_price             4675 non-null   float64       
     30  products.price                 4675 non-null   float64       
     31  products.product_id            4675 non-null   int64         
     32  products.product_name          4675 non-null   object        
     33  products.quantity              4675 non-null   int64         
     34  products.sku                   4675 non-null   object        
     35  products.tax_amount            4675 non-null   float64       
     36  products.taxful_price          4675 non-null   float64       
     37  products.taxless_price         4675 non-null   float64       
     38  products.unit_discount_amount  4675 non-null   float64       
     39  sku                            4675 non-null   object        
     40  taxful_total_price             4675 non-null   float64       
     41  taxless_total_price            4675 non-null   float64       
     42  total_quantity                 4675 non-null   int64         
     43  total_unique_products          4675 non-null   int64         
     44  type                           4675 non-null   object        
     45  user                           4675 non-null   object        
    dtypes: datetime64[ns](3), float64(12), int64(5), object(26)
    memory usage: 64.0 bytes
    


```python
# getting the dtypes from pandas dataframe:
df_ecommerce.dtypes
```




    category                  object
    currency                  object
    customer_first_name       object
    customer_full_name        object
    customer_gender           object
    customer_id                int64
    customer_last_name        object
    customer_phone            object
    day_of_week               object
    day_of_week_i              int64
    email                     object
    manufacturer              object
    order_date                object
    order_id                   int64
    products                  object
    sku                       object
    taxful_total_price       float64
    taxless_total_price      float64
    total_quantity             int64
    total_unique_products      int64
    type                      object
    user                      object
    geoip                     object
    event                     object
    dtype: object




```python
# retrieving the Data types for the index normally would require us to perform the following Elasticsearch query:
mapping = es.indices.get_mapping(index_name) 
# which by itself is an abstraction of the GET request for mapping retrieval
json(mapping)
```

    {
      "kibana_sample_data_ecommerce": {
        "mappings": {
          "properties": {
            "category": {
              "fields": {
                "keyword": {
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "currency": {
              "type": "keyword"
            },
            "customer_birth_date": {
              "type": "date"
            },
            "customer_first_name": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "customer_full_name": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "customer_gender": {
              "type": "keyword"
            },
            "customer_id": {
              "type": "keyword"
            },
            "customer_last_name": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "customer_phone": {
              "type": "keyword"
            },
            "day_of_week": {
              "type": "keyword"
            },
            "day_of_week_i": {
              "type": "integer"
            },
            "email": {
              "type": "keyword"
            },
            "event": {
              "properties": {
                "dataset": {
                  "type": "keyword"
                }
              }
            },
            "geoip": {
              "properties": {
                "city_name": {
                  "type": "keyword"
                },
                "continent_name": {
                  "type": "keyword"
                },
                "country_iso_code": {
                  "type": "keyword"
                },
                "location": {
                  "type": "geo_point"
                },
                "region_name": {
                  "type": "keyword"
                }
              }
            },
            "manufacturer": {
              "fields": {
                "keyword": {
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "order_date": {
              "type": "date"
            },
            "order_id": {
              "type": "keyword"
            },
            "products": {
              "properties": {
                "_id": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "base_price": {
                  "type": "half_float"
                },
                "base_unit_price": {
                  "type": "half_float"
                },
                "category": {
                  "fields": {
                    "keyword": {
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "created_on": {
                  "type": "date"
                },
                "discount_amount": {
                  "type": "half_float"
                },
                "discount_percentage": {
                  "type": "half_float"
                },
                "manufacturer": {
                  "fields": {
                    "keyword": {
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "min_price": {
                  "type": "half_float"
                },
                "price": {
                  "type": "half_float"
                },
                "product_id": {
                  "type": "long"
                },
                "product_name": {
                  "analyzer": "english",
                  "fields": {
                    "keyword": {
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "quantity": {
                  "type": "integer"
                },
                "sku": {
                  "type": "keyword"
                },
                "tax_amount": {
                  "type": "half_float"
                },
                "taxful_price": {
                  "type": "half_float"
                },
                "taxless_price": {
                  "type": "half_float"
                },
                "unit_discount_amount": {
                  "type": "half_float"
                }
              }
            },
            "sku": {
              "type": "keyword"
            },
            "taxful_total_price": {
              "type": "half_float"
            },
            "taxless_total_price": {
              "type": "half_float"
            },
            "total_quantity": {
              "type": "integer"
            },
            "total_unique_products": {
              "type": "integer"
            },
            "type": {
              "type": "keyword"
            },
            "user": {
              "type": "keyword"
            }
          }
        }
      }
    }
    


```python
# Eland abstracts this procedure into the same pandas api:
ed_ecommerce.dtypes
```




    category                                 object
    currency                                 object
    customer_birth_date              datetime64[ns]
    customer_first_name                      object
    customer_full_name                       object
    customer_gender                          object
    customer_id                              object
    customer_last_name                       object
    customer_phone                           object
    day_of_week                              object
    day_of_week_i                             int64
    email                                    object
    event.dataset                            object
    geoip.city_name                          object
    geoip.continent_name                     object
    geoip.country_iso_code                   object
    geoip.location                           object
    geoip.region_name                        object
    manufacturer                             object
    order_date                       datetime64[ns]
    order_id                                 object
    products._id                             object
    products.base_price                     float64
    products.base_unit_price                float64
    products.category                        object
    products.created_on              datetime64[ns]
    products.discount_amount                float64
    products.discount_percentage            float64
    products.manufacturer                    object
    products.min_price                      float64
    products.price                          float64
    products.product_id                       int64
    products.product_name                    object
    products.quantity                         int64
    products.sku                             object
    products.tax_amount                     float64
    products.taxful_price                   float64
    products.taxless_price                  float64
    products.unit_discount_amount           float64
    sku                                      object
    taxful_total_price                      float64
    taxless_total_price                     float64
    total_quantity                            int64
    total_unique_products                     int64
    type                                     object
    user                                     object
    dtype: object




```python
# defining the full-text query we need: Retrieving records for either Elitelligence or Primemaster manufacturer
query = {
        "query_string" : {
            "fields" : ["manufacturer"],
            "query" : "Elitelligence OR Primemaster"
        }
    }
# using full-text search capabilities with Eland:
text_search_df = ed_ecommerce.es_query(query)

# visualizing price of products for each manufacturer using pandas column syntax:
text_search_df[['manufacturer','products.price']]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>manufacturer</th>
      <th>products.price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Fy_X0nMBxx5L21Ced97s</th>
      <td>[Elitelligence, Oceanavigations]</td>
      <td>[11.99, 24.99]</td>
    </tr>
    <tr>
      <th>Gi_X0nMBxx5L21Ced97t</th>
      <td>[Primemaster, Oceanavigations]</td>
      <td>[74.99, 99.99]</td>
    </tr>
    <tr>
      <th>Gy_X0nMBxx5L21Ced97t</th>
      <td>[Elitelligence]</td>
      <td>[59.99, 20.99]</td>
    </tr>
    <tr>
      <th>HS_X0nMBxx5L21Ced97t</th>
      <td>[Low Tide Media, Elitelligence]</td>
      <td>[20.99, 24.99]</td>
    </tr>
    <tr>
      <th>Hi_X0nMBxx5L21Ced97t</th>
      <td>[Low Tide Media, Oceanavigations, Elitelligence]</td>
      <td>[28.99, 41.99, 59.99, 7.99]</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>SC_X0nMBxx5L21CejPB3</th>
      <td>[Elitelligence]</td>
      <td>[59.99, 18.99]</td>
    </tr>
    <tr>
      <th>Sy_X0nMBxx5L21CejPB3</th>
      <td>[Low Tide Media, Elitelligence]</td>
      <td>[24.99, 41.99]</td>
    </tr>
    <tr>
      <th>Ty_X0nMBxx5L21CejPB3</th>
      <td>[Elitelligence]</td>
      <td>[24.99, 25.99]</td>
    </tr>
    <tr>
      <th>US_X0nMBxx5L21CejPB3</th>
      <td>[Angeldale, Elitelligence]</td>
      <td>[79.99, 10.99]</td>
    </tr>
    <tr>
      <th>VS_X0nMBxx5L21CejPB3</th>
      <td>[Elitelligence, Low Tide Media]</td>
      <td>[7.99, 59.99]</td>
    </tr>
  </tbody>
</table>
</div>
<p>1435 rows × 2 columns</p>




```python

```
