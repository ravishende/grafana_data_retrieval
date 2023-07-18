import json
import requests
from bs4 import BeautifulSoup
from utils import *

print("\n\n\n\n")
print_query_values(True)
print("\n\n\n\n")


# json_print()


# url = "https://grafana.nrp-nautilus.io/d/85a562078cdf77779eaa1add43ccec1e/kubernetes-compute-resources-namespace-pods?orgId=1&var-datasource=thanos&var-cluster=&var-namespace=wifire&from=1756419957757&to=1806913309834"
# all_data = requests.get(url)

# s = BeautifulSoup(all_data.content, "html.parser")
# print(s)
# print("successfully requested url")

# print(s.text)
# res = s.find(id="containername")
# title = s.find("h2",class_="title is-5")
# print(title[0].text)
