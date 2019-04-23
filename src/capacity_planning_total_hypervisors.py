#!/usr/bin/python3

import sys
import json
import logging
import traceback
import datetime
from time import gmtime, strftime
import os
import requests

"""
Author : Julie Daligaud <julie.daliugaud@gmail.com>
"""

def send_to_elk(url, data_json):
    """
    Send data formated in JSON to the elastic search stack.
    """

    try:
        requests.post(url, data=data_json, timeout=5)
    except RequestException:
        message = "Error while sending data to elasticsearch at " + url
        sys.exit(message)


def request(json_value):
    """ Request from ELK. """

    url = ELK_URL + "/" + MAIN_INDEX + "/" + "_search"
    req = requests.get(url, data=json_value, timeout=5)

    if req.status_code != 200:
        message = "Error while requesting object"
        logging.warning(str(message + traceback.format_exc()))
        sys.exit(message)

    return json.loads(req.content)


def request_filter(filter_values):
    """
    Request ELK stack with a filter.
    The filter must be a list of map.

    ALL THE AVERAGE ARE DONE ON A PERIODE OF 24 HOURS.

    """

    # "must" :[{"term":{"_type":""}}, {"term" : {"name": ""}}],
    search_json = """{
        "query" : {
            "bool" : {
                "must" :[],
                "filter": {
                    "range": {
                        "post_date": {
                            "gt": "now-24h"
                        }
                    }
                }
            }
        }
    }
    """
    search = json.loads(search_json)

    for filter_value in filter_values:
        search['query']['bool']['must'].append({'term': filter_value})
    search_json = json.dumps(search)

    return request(search_json)


def request_by_name(typeValue, nameValue):
    return request_filter([{'_type': typeValue}, {'name': nameValue}])


def request_hosts_in_cluster(cluster):
    """ Request all the host in a cluster from ELK.  """

    dc_query = request_filter([{'_type': HV_INDEX}, {'cluster': cluster}])
    hosts = {}

    for hit in dc_query['hits']['hits']:
        hosts[hit['_source']['name']] = 1
    result = []

    for key in list(hosts.keys()):
        result.append(key)

    return result


def sum_by_cluster(cluster, value):
    """ Return the sum of field from all hosts of a cluster.  """

    hosts = request_hosts_in_cluster(cluster)
    result = 0.0

    # Remove one hypervisor from capacity-planning for spare.
    # 2018-04-17: we won't remove this hypervisor.
    #if value in ('pRAMfree', 'vRAMfree', 'vCPUfree') and len(hosts) > 1:
    #    hosts.pop(len(hosts) - 1)

    for host in hosts:
        result += average_by_name(host, value)

    return result


def average_by_name(name, value):
    """ Returns the average of a given field by host name. """

    hits = request_by_name(HV_INDEX, name)
    hits_cpt = 0.0
    hits_sum = 0.0

    for hit in hits['hits']['hits']:
        hits_sum += float(hit['_source'][value])
        hits_cpt += 1.0
    if hits_cpt <= 0:
        return 0.0

    return float(hits_sum / hits_cpt)


def send_sums_by_cluster(cluster):
    """ Process data per cluster and send results to ELK. """

    cluster_data = {}
    cluster_data['name'] = cluster
    cluster_data['pRAMfree'] = sum_by_cluster(cluster, "pRAMfree")
    cluster_data['pRAMtotal'] = sum_by_cluster(cluster, "pRAMtotal")
    cluster_data['pRAMused'] = sum_by_cluster(cluster, "pRAMused")
    cluster_data['vRAMfree'] = sum_by_cluster(cluster, "vRAMfree")
    cluster_data['vRAMallocated'] = sum_by_cluster(cluster, "vRAMallocated")
    cluster_data['pCPU'] = sum_by_cluster(cluster, "pCPU")
    cluster_data['vCPUfree'] = sum_by_cluster(cluster, "vCPUfree")
    cluster_data['vCPUallocated'] = sum_by_cluster(cluster, "vCPUallocated")

    if cluster_data['pRAMtotal'] > 0.0 and cluster_data['vRAMallocated'] > 0.0:
        cluster_data['RAMratio'] = float(float(cluster_data['vRAMallocated']) /
                                         (float(cluster_data['pRAMtotal']) *
                                          float(RAM_OVERCOMMIT / 100.0))) * \
                                          100.0
    else:
        cluster_data['pRAMtotal'] = 0.0

    if cluster_data['pCPU'] > 0.0 and cluster_data['vCPUallocated'] > 0.0:
        cluster_data['CPUratio'] = float(float(cluster_data['vCPUallocated']) /
                                         (float(cluster_data['pCPU']) *
                                          float(CPU_OVERCOMMIT / 100.0))) * \
                                          100.0
    else:
        cluster_data['CPUratio'] = 0.0

    # Calculate how many vm we can fit in our clusters
    for vm_type in VMS_TYPE:
        if 'type' in vm_type:
            if int(vm_type['cpu']) > 0 and int(vm_type['ram']) > 0:
                vm_for_cpu = int(cluster_data['vCPUfree']) / int(vm_type['cpu'])
                vm_for_ram = int(cluster_data['vRAMfree']) / int(vm_type['ram'])

                cluster_data['remaining_vm_type_' + vm_type['type']] = \
                    max(vm_for_cpu, vm_for_ram)

    cluster_data['post_date'] = NOW.isoformat()
    cluster_data_json = json.dumps(cluster_data)

    send_to_elk(ELK_URL + "/" + MAIN_INDEX + "/" + CLUSTER_INDEX,
                cluster_data_json)


def parse_conf():
    """
    Parse the JSON configuration file nad return a map.
    """
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    # Parse conf file
    try:
        conf_file = open(os.path.join(__location__, "capacityPlanning.json"))
        conf = conf_file.read()
        conf_file.close()
    except (OSError, IOError):
        sys.exit("Error while loading conf file." + traceback.format_exc())

    try:
        conf = json.loads(conf)
    except ValueError:
        sys.exit("Error while parsing conf file." + traceback.format_exc())

    return conf

if __name__ == "__main__":
    CONF = parse_conf()

    LOGFILE = CONF['logs']
    ELK_URL = CONF['url']
    MAIN_INDEX = CONF['indexes']['main']
    CLUSTER_INDEX = CONF['indexes']['clusters']
    HV_INDEX = CONF['indexes']['hv']
    LOGFILE = CONF['logs']
    CPU_OVERCOMMIT = float(CONF['hv_cpu_overcommit'])
    RAM_OVERCOMMIT = float(CONF['hv_ram_overcommit'])
    VMS_TYPE = CONF['vm_type']
    ###

    NOW = datetime.datetime.NOW()

    LOGFILE = LOGFILE + ".log"
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG)
    logging.info(str(strftime("\n\n-----\n" + "%Y-%m-%d %H:%M:%S", gmtime()) +
                     " : Starting capacity planning script."))

    send_sums_by_cluster("ven-mut")
    send_sums_by_cluster("pa2-mut")
