#!/usr/bin/python3

"""
Author : Julie Daligaud <julie.daligaud@gmail.com>

Script that fetches SNMP stats on SANs
and send it to elastic search
"""

import json
import datetime
import logging
import traceback
from time import gmtime, strftime
import sys
import os
from pysnmp.hlapi import *
import requests


def mib_to_gib(value):
    """
    Returns value in Gib.
    """
    return float(float(value) / 1024.0)


def get_oid_num(string):
    """ Extracts iod number from a snmpget result. """

    oid_num = str(string).split('=')[0].strip()
    oid_num = oid_num.split('.')

    return oid_num[len(oid_num) - 1]


def send_to_elk(url, data_map):
    """ Sends data to the ELK stack. """

    now = datetime.datetime.now()
    tmp_data = data_map.copy()
    tmp_data['post_date'] = now.isoformat()
    data_json = json.dumps(tmp_data)

    try:
        requests.post(url, data=data_json, timeout=5)
    except RequestException:
        message = "Error while sending data to elasticsearch at " + url
        logging.warning(str(message + traceback.format_exc()))
        sys.exit(message)


def walk(host, oid):
    """
    Does a snmpwalk on host starting from given oid number.
    returns an array of unformated results.
    """

    binds = []

    for (error_indication, error_status, error_index, var_binds) \
        in nextCmd(SnmpEngine(),
                   CommunityData(SNMP_COMMUNITY),
                   UdpTransportTarget((host, 161)),
                   ContextData(),
                   ObjectType(ObjectIdentity(oid)),
                   lexicographicMode=False):
        if error_indication:
            print(error_indication)
        elif error_status:
            print(('%s at %s' % (
                error_status.prettyPrint(),
                error_index and var_binds[
                    int(error_index) - 1][0] or '?'
                )

                  ))
        else:
            for var_bind in var_binds:
                res = str(' = '.join([x.prettyPrint() for x in var_bind]))
                binds.append(res)

    return binds


def get(host, oid):
    """
    Does a snmpget on host for the given oid number.
    Returns a formated result with only the value needed.
    """

    get_cmd = getCmd(SnmpEngine(),
                     CommunityData(SNMP_COMMUNITY),
                     UdpTransportTarget((host, 161)),
                     ContextData(),
                     ObjectType(ObjectIdentity(oid)))

    error_indication, error_status, error_index, var_binds = next(get_cmd)

    if error_indication:
        print(error_indication)
    elif error_status:
        print(('%s at %s' % (
            error_status.prettyPrint(),
            error_index and var_binds[
                int(error_index) - 1][0] or '?'
            )

              ))
    else:
        for var_bind in var_binds:
            res = ' = '.join([x.prettyPrint() for x in var_bind])
            return res.split('=')[1].strip()

    return None


def list_pools(host):
    """
    Lists pools in a given SAN group (host).
    Returns an array with their names.
    Pools "default" are excluded.
    """

    res = {}
    pools = walk(host, '1.3.6.1.4.1.12740.16.1.1.1.3.1')

    for pool in pools:
        name = str(pool).split('=')[1].strip()
        if name != "default":
            res[get_oid_num(str(pool))] = name

    return res


def get_stat_on_pools(host, pools, oid, to_gib):
    """
    Fetches a specific stat of identified by the "oid" parameter
    on given dict {"oid pool", "name"} of pools.
    Returns a dict {"oid pool", "value"} with this stat.
    If "toGib" is True, parse it from Kib to Gib.
    """

    res = {}

    for oid_num in list(pools.items()):
        value = get(host, oid + str(oid_num))
        if to_gib:
            value = mib_to_gib(value)
        res[str(oid_num)] = value

    return res


def get_stats_on_all_pools(host, cluster, datacenter, send):
    """
    Fetches all stats on all pools on a given SAN group "host".
    Returns an array of dicts [{"oid pool", {"stat name", "value}}].
    If "send" is True, send these to the ELK stack.
    """

    pools = list_pools(host)

    # data is a 2 dimension dictionnary structured as below :
    # data{StatName}{oid_numPool}
    data = {}
    data['SANCountVol'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.16.1.",
                          False)
    data['SANTotalVol'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.1.1.",
                          True)
    data['SANFreeVol'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.3.1.",
                          True)
    data['SANTotalReplication'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.4.1.",
                          True)
    data['SANUsedReplication'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.5.1.",
                          True)
    data['SANFreeReplication'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.6.1.",
                          True)
    data['SANReservedSnapshot'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.9.1.",
                          True)
    data['SANUsedSnapshot'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.10.1.",
                          True)
    data['SANTotalDelegatedSpace'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.17.1.",
                          True)
    data['SANUsedDelegatedSpace'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.18.1.",
                          True)
    data['SANAllocatedVolSpace'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.21.1.",
                          True)
    data['SANFreeThinProv'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.23.1.",
                          True)
    data['SANFreeSnaphot'] = \
        get_stat_on_pools(host, pools,
                          "1.3.6.1.4.1.12740.16.1.2.1.25.1.",
                          True)

    # Process used volume and ratio form fetched stats to avoid
    # doing this with scripted fields in the ELK stack.
    data['SANUsedVol'] = {}
    data['SANVolRatio'] = {}
    data['SANPoolsUsage'] = {}

    for oid_num in list(data['SANTotalVol'].items()):
        data['SANUsedVol'][oid_num] = \
            float(data['SANTotalVol'][oid_num] - data['SANFreeVol'][oid_num])
        data['SANVolRatio'][oid_num] = \
            float(data['SANUsedVol'][oid_num] / data['SANTotalVol'][oid_num] *
                  100.0)

        # If there aren't any volumes on a pool, this pool is considered
        # dedicated to the replication.
        if int(data['SANCountVol'][oid_num]) > 0:
            data['SANPoolsUsage'][oid_num] = "storage"
        else:
            data['SANPoolsUsage'][oid_num] = "replication"

    # Construct the result array from fetched data.
    # Send these data to the ELK stack if needed.
    res = []

    for oid_num, pool in list(pools.items()):
        pool_data = {}
        pool_data['name'] = pool
        pool_data['host'] = host
        pool_data['cluster'] = cluster
        pool_data['datacenter'] = datacenter
        for data_name, data_pools in list(data.items()):
            pool_data[data_name] = data_pools[oid_num]
        if send:
            send_to_elk(ELK_URL + "/" + MAIN_INDEX + "/" + POOLS_INDEX,
                        pool_data)
        res.append(pool_data)

    return res


def is_aggretable_stat(stat_name):
    """ Do we want to aggregate this stat in host, cluster and dc ? """

    return stat_name not in ('name', 'host', 'cluster',
                             'datacenter', 'SANPoolsUsage')\
        and 'Ratio' not in stat_name


def agg_stats(data):
    """
    Aggregates (sum) all given data (for host, cluster and dc)
    to avoid doing it in the ELK stack.
    Returns a dict {"stat name", "value"} with aggregated stats.
    """

    res = {}

    if data:
        for stat_name  in list(data[0].items()):
            if is_aggretable_stat(stat_name):
                res[stat_name] = 0.0
                for entry in data:
                    if stat_name in entry:
                        res[stat_name] += float(entry[stat_name])

    return res


def exlude_replication_pools(pools_data):
    """
    Returns an array without pools only used for replication.
    """

    data = []

    for entry in pools_data:
        if entry['SANPoolsUsage'] != "replication":
            data.append(entry.copy())

    return data


def get_stats_on_all_hosts(data, cluster, datacenter, send):
    """
    Fetches stats on all hosts in a given cluster.
    Returns an array of a dict [{"host name", {"stat name", "value"}}];
    If "send" is True, sends these stats on the ELK stack.
    """

    res = []

    for host in data[datacenter][cluster]:
        pools_data = get_stats_on_all_pools(host, cluster, datacenter, True)
        pools_data = exlude_replication_pools(pools_data)
        host_data = agg_stats(pools_data)
        host_data['name'] = host
        host_data['cluster'] = cluster
        host_data['datacenter'] = datacenter
        if 'SANUsedVol' in host_data and 'SANTotalVol' in host_data:
            host_data['SANVolRatio'] = float(host_data['SANUsedVol'] /
                                             host_data['SANTotalVol'] * 100.0)
        else:
            # If an host is dedicated to the replication, its stats are 0.0
            host_data['SANVolRatio'] = 0.0
            host_data['SANTotalVol'] = 0.0
            host_data['SANUsedVol'] = 0.0
            host_data['SANFreeVol'] = 0.0

        # subtract 5% of free vol on each SAN to
        # prevent performance degradation
        host_data['SANFreeVol'] = host_data['SANFreeVol'] - \
            (host_data['SANTotalVol'] * 5.0 / 100.0)

        if send:
            send_to_elk(ELK_URL + "/" + MAIN_INDEX + "/" + HOSTS_INDEX,
                        host_data)
        res.append(host_data)

    return res


def get_stats_on_all_clusters(data, datacenter, send):
    """
    Fetches stats on all clusters in a given datacenter.
    Returns an array of a dict [{"cluster name", {"stat name", "value"}}];
    If "send" is True, sends these stats on the ELK stack.
    """

    res = []

    for cluster in data[datacenter]:
        hosts_data = get_stats_on_all_hosts(data, cluster, datacenter, True)
        cluster_data = agg_stats(hosts_data)
        cluster_data['name'] = cluster
        cluster_data['datacenter'] = datacenter
        if 'SANUsedVol' in cluster_data and 'SANTotalVol' in cluster_data:
            cluster_data['SANVolRatio'] = \
                float(cluster_data['SANUsedVol'] /
                      cluster_data['SANTotalVol'] * 100.0)
        if send:
            send_to_elk(ELK_URL + "/" + MAIN_INDEX + "/" + CLUSTERS_INDEX,
                        cluster_data)
        res.append(cluster_data)

    return res


def get_stats_on_all_datacenters(data, send):
    """
    Fetches stats on all datacenters.
    Returns an array of a dict [{"cluster name", {"stat name", "value"}}];
    If "send" is True, sends these stats on the ELK stack.
    """

    res = []

    for datacenter in data:
        cluster_data = get_stats_on_all_clusters(data, datacenter, True)
        dc_data = agg_stats(cluster_data)
        dc_data['name'] = datacenter
        if 'SANUsedVol' in dc_data and 'SANTotalVol' and dc_data:
            dc_data['SANVolRatio'] = \
                float(dc_data['SANUsedVol'] /
                      dc_data['SANTotalVol'] * 100.0)
        if send:
            send_to_elk(ELK_URL + "/" + MAIN_INDEX + "/" + DC_INDEX, dc_data)
        res.append(dc_data)

    return res


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
    POOLS_INDEX = CONF['indexes']['san_pools']
    HOSTS_INDEX = CONF['indexes']['san_hosts']
    DC_INDEX = CONF['indexes']['san_dc']
    CLUSTERS_INDEX = CONF['indexes']['san_clusters']
    SNMP_COMMUNITY = CONF['snmp_community']
    MAP_SAN = CONF['san']

    LOGFILE = LOGFILE + ".log"
    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG)
    logging.info(str(strftime("\n\n-----\n" + "%Y-%m-%d %H:%M:%S", gmtime()) +
                     " : Starting capacity planning script."))

    get_stats_on_all_datacenters(MAP_SAN, True)
