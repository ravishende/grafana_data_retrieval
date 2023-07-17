#import statements
import requests


base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
query = 'rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[3h])'
endpoint = f'query?query={query}'
full_url = base_url + endpoint

cpu_data = requests.get(full_url)

print("cpu data is", cpu_data)

print("\n\n\n**********")
print("full_url is\n", full_url) 
print("\n\nendpoint is\n", endpoint)
# print("\n\fcpu_data is\n", cpu_data)
print("***********\n\n")


x = requests.get('https://w3schools.com')
print("cpu status code is\n", cpu_data.status_code)
print("\n\ncpu's dir is", dir(cpu_data))
print("\n\n\n")

