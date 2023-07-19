*** Please tell me who you are.

Run

  git config --global user.email "you@example.com"
  git config --global user.name "Your Name"

to set your account's default identity.
Omit --global to set the identity only in this repository.

fatal: empty ident name (for <devin@zorak.nic.uoregon.edu>) not allowed
[devin@zorak grafana_data_retrieval]$ git push

(gnome-ssh-askpass:2594016): Gtk-WARNING **: 19:54:53.486: cannot open display: 
error: unable to read askpass response from '/usr/libexec/openssh/gnome-ssh-askpass'
Username for 'https://github.com': devinshende

(gnome-ssh-askpass:2594044): Gtk-WARNING **: 19:54:57.223: cannot open display: 
error: unable to read askpass response from '/usr/libexec/openssh/gnome-ssh-askpass'
Password for 'https://devinshende@github.com': 
remote: Support for password authentication was removed on August 13, 2021.
remote: Please see https://docs.github.com/en/get-started/getting-started-with-git/about-remote-repositories#cloning-with-https-urls for information on currently recommended modes of authentication.
fatal: Authentication failed for 'https://github.com/ravishende/grafana_data_retrieval.git/'
[devin@zorak grafana_data_retrieval]$ git push

(gnome-ssh-askpass:2594072): Gtk-WARNING **: 19:55:12.431: cannot open display: 
error: unable to read askpass response from '/usr/libexec/openssh/gnome-ssh-askpass'
Username for 'https://github.com': devinshende

(gnome-ssh-askpass:2594077): Gtk-WARNING **: 19:55:15.267: cannot open display: 
error: unable to read askpass response from '/usr/libexec/openssh/gnome-ssh-askpass'
Password for 'https://devinshende@github.com': 
remote: Support for password authentication was removed on August 13, 2021.
remote: Please see https://docs.github.com/en/get-started/getting-started-with-git/about-remote-repositories#cloning-with-https-urls for information on currently recommended modes of authentication.
fatal: Authentication failed for 'https://github.com/ravishende/grafana_data_retrieval.git/'
[devin@zorak grafana_data_retrieval]$ git push

(gnome-ssh-askpass:2594100): Gtk-WARNING **: 19:55:22.947: cannot open display: 
error: unable to read askpass response from '/usr/libexec/openssh/gnome-ssh-askpass'
Username for 'https://github.com': devinshende

(gnome-ssh-askpass:2594114): Gtk-WARNING **: 19:55:28.267: cannot open display: 
error: unable to read askpass response from '/usr/libexec/openssh/gnome-ssh-askpass'
Password for 'https://devinshende@github.com': 
remote: Support for password authentication was removed on August 13, 2021.
remote: Please see https://docs.github.com/en/get-started/getting-started-with-git/about-remote-repositories#cloning-with-https-urls for information on currently recommended modes of authentication.
fatal: Authentication failed for 'https://github.com/ravishende/grafana_data_retrieval.git/'
[devin@zorak grafana_data_retrieval]$ clear

[devin@zorak grafana_data_retrieval]$ vim main.py 
[devin@zorak grafana_data_retrieval]$ vim utils.py 


















































#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
import json
from termcolor import cprint, colored
from pprint import pprint

ts = '[1h]'
#TODO: handle time inputs for data collection

QUERY_COUNT = 0

#retrieves information from the 4 panels under headlines (cpu and memory utilisation data)
QUERIES = {
    'CPU Utilisation (from requests)': 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"})',
    'CPU Utilisation (from limits)': 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"})',
    'Memory Utilisation (from requests)': 'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", image!=""}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="memory"})',
    'Memory Utilisation (from limits)': 'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", image!=""}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="memory"})',
    }

def get_pods_list():
        data = query_api_site('rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[3h])')
        try:
                pods = data["data"]["result"]
                pods_list = []
                for pod in pods:
                        pods_list.append(pod["metric"]["pod"])
        except KeyError as e:
                return ["Error retrieving pods"]
        return pods_list


#parses json for numerical data values
def query_value(query):
    #get json
    api_response = query_api_site(query)

    #get result list
    result_list = get_result_list(api_response)

    #there is a bug with the api itself where every fifth request comes back with no data
    if(len(result_list) == 0):
        #the fix to this is simply to regenerate the response if it comes back empty.    
        api_response = query_api_site(query)
        return get_result(get_result_list(api_response))

    #if result already has data, just return the result 
"utils.py" 190L, 6967C                                                                                                                                                        32,0-1        Top
