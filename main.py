import json
import requests
from bs4 import BeautifulSoup
from utils import *
import pandas as pd

print("\n\n")

column_names=["CPU Usage","CPU Requests",'CPU Requests %', 'CPU Limits']
data = [ ["Spark",20000, "30days", 0], 
         ["Pandas",25000, "40days", 0], ]

df = pd.DataFrame(data, columns=column_names)


d = {'CPU Usage': 3, 'CPU Requests':3, 'CPU Requests %': 89, 'CPU Limits': 93}
df = df.append(d, ignore_index = True)
print(df)