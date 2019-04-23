#!/usr/bin/python3


"""
Author : Julie Daligaud <julie.daligaud@gmail.com>

Script that fetches data on ELK concorning the backups
compile it and send the average back to ELK.
"""

import sys
import json
import logging
import traceback
import datetime
from time import gmtime, strftime
import os
import requests


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
    """ Request values from ES. """
    req = requests.get(ELK_URL + "/" + MAIN_INDEX + "/" + "_search",
                       data=json_value, timeout=5)
    if req.status_code != 200:
        message = "Error while requesting object"
        logging.warning(str(message + traceback.format_exc()))
        sys.exit(message)

    return json.loads(req.content)


def request_filter(filter_values):
    """Request ELK stack with a filter.
        The filter must be a list of map"""

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


def request_by_name(type_value, name_value):
    """ Request value from ES for a host name. """
    return request_filter([{'_type': type_value}, {'name': name_value}])


def request_bc_host_in_dc(datacenter):
    """ Return a list of backupHosts in a datacenter"""

    dc_query = request_filter([{'_type': 'backuphost'},
                               {'datacenter': datacenter}])
    hosts = {}
    for hit in dc_query['hits']['hits']:
        hosts[hit['_source']['name']] = 1
    result = []
    for key in list(hosts.keys()):
        result.append(key)
    return result


def average_by_name(name, value):
    """ Returns the average for a host name. """

    hits = request_by_name("backuphost", name)
    hits_cpt = 0.0
    hits_sum = 0.0
    for hit in hits['hits']['hits']:
        hits_sum += float(hit['_source'][value])
        hits_cpt += 1.0
    if hits_cpt <= 0:
        return 0.0

    return float(hits_sum / hits_cpt)


def sum_by_dc(datacenter, value):
    """ Returns the average by host name in a datacenter. """
    hosts = request_bc_host_in_dc(datacenter)
    result = 0.0
    for host in hosts:
        result += average_by_name(host, value)
    return result


def send_sums_by_dc(datacenter):
    """ Send a doc with the sums of volumes by DC """
    dc_data = {}
    dc_data['name'] = datacenter
    dc_data['volumeLogUsed'] = sum_by_dc(datacenter, "volumeLogUsed")
    dc_data['volumeLogFree'] = sum_by_dc(datacenter, "volumeLogFree")
    dc_data['volumeUsed'] = sum_by_dc(datacenter, "volumeUsed")
    dc_data['volumeFree'] = sum_by_dc(datacenter, "volumeFree")
    dc_data['volumeTotal'] = sum_by_dc(datacenter, "volumeTotal")
    dc_data['post_date'] = NOW.isoformat()

    if float(dc_data['volumeTotal']) <= 0.0:
        print(("Error vol total on : " + dc_data['name']))
    else:
        dc_data['volumeRatio'] = float(dc_data['volumeUsed']) / \
                                       float(dc_data['volumeTotal']) * 100.0

    dc_data_json = json.dumps(dc_data)
    send_to_elk(ELK_URL + "/" + MAIN_INDEX + "/" + BACKUPDC_INDEX, dc_data_json)


def parse_conf():
    """
    Parse the JSON configuration file and return a map.
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
    BACKUPDC_INDEX = CONF['indexes']['backup_dc']
    # End parse conf file

    NOW = datetime.datetime.now()

    LOGFILE = LOGFILE + ".log"
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG)
    logging.info(str(strftime("\n\n-----\n" + "%Y-%m-%d %H:%M:%S", gmtime()) +
                     " : Starting capacity planning script."))

    send_sums_by_dc("ven")
    send_sums_by_dc("eqx")
