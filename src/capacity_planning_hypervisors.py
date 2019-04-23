#!/usr/bin/python3

"""
Author : Julie Daligaud <julie.daligaud@gmail.com>

Script that fetches stats on the virtual host and virtual machines
and send it to elastic search.
"""

from subprocess import call, Popen, PIPE
import os
import json
import datetime
import logging
import traceback
from time import gmtime, strftime
import sys
import requests


def call_cmd(cmd):
    """ Call a command line and return the result as a string. """

    try:
        child = Popen(list(str(cmd).split(' ')), stdout=PIPE)
        string = child.communicate()[0]
        child.stdout.close()
    except OSError:
        message = str("Error while executing " + cmd + "\n" + traceback.format_exc())
        logging.warning(message)
        sys.exit(message)

    return string.decode()



def kib_to_gib(value):
    """ Take kib and return the value in gib. """

    return int(float(float(value) / 1024.0) / 1024.0)


def send_to_elk(url, data_json):
    """
    Send data formated in JSON to the elastic search stack.
    """

    try:
        requests.post(url, data=data_json, timeout=5)
    except RequestException:
        message = "Error while sending data to elasticsearch at " + url
        sys.exit(message)


def main():
    """ Main function. """

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
    path_stats = "/tmp/capacity_planning"
    path_script = os.path.join(conf['working_dir'], "stats.sh")
    logfile = conf['logs']
    elk_url = conf['url']
    main_index = conf['indexes']['main']
    vm_index = conf['indexes']['vm']
    hv_index = conf['indexes']['hv']
    cluster = conf['cluster']
    cpu_overcommit = int(conf['hv_cpu_overcommit'])
    ram_overcommit = int(conf['hv_ram_overcommit'])
    # End parse conf file

    now = datetime.datetime.now()

    logfile = logfile + ".log"
    logging.basicConfig(filename=logfile, level=logging.DEBUG)
    logging.info(str(strftime("\n\n-----\n" + "%Y-%m-%d %H:%M:%S", gmtime()) +
                     " : Starting capacity planning script."))

    # Get name and domain of host (fqdn)
    hostname = call_cmd("hostname")
    domain = call_cmd("hostname -d")
    hostname = hostname.strip()
    fqdn = str(hostname) + "." + str(domain).strip()

    pram_free = call_cmd("grep MemFree /proc/meminfo")
    pram_free = int(str(str(pram_free.split(':')[1]).split('k')[0]).strip())
    pram_cache = call_cmd("grep Cached /proc/meminfo")
    pram_cache = int(str(str(pram_cache.split(':')[1]).split('k')[0]).strip())
    pram_buffer = call_cmd("grep Buffers /proc/meminfo")
    pram_buffer = int(str(str(pram_buffer.split(':')[1]).split('k')[0]).strip())
    pram_slab = call_cmd("grep Slab /proc/meminfo")
    pram_slab = int(str(str(pram_slab.split(':')[1]).split('k')[0]).strip())
    pram_free += pram_buffer + pram_slab + pram_cache
    pram_total = call_cmd("grep MemTotal /proc/meminfo")
    pram_total = int(str(str(pram_total.split(':')[1]).split('k')[0]).strip())
    pram_used = pram_total - pram_free
    host_vram_alloc = 0

    pcpu = call_cmd("nproc")
    pcpu = int(pcpu)
    host_cpu_allocated = 0

    host_data = {
        'name':  fqdn,
        'pRAMfree': kib_to_gib(pram_free),
        'pRAMused': kib_to_gib(pram_used),
        'pRAMtotal': kib_to_gib(pram_total),
        'pCPU': pcpu,
        'post_date': now.isoformat(),
        'cluster': cluster
    }

    # Call script who create one file for each vm that run on the host
    try:
        call(["/bin/sh", path_script])
    except OSError:
        message = "Error while executing " + path_script
        logging.warning(str(message + traceback.format_exc()))
        sys.exit(message)

    # Push VM info in ELK
    path_stats_files = os.path.join(path_stats, fqdn + "-vms")
    vm_stat_files = [f for f in os.listdir(path_stats_files)
                     if os.path.isfile(os.path.join(path_stats_files, f))]

    for stat_file in vm_stat_files:
        data = {}
        data['host'] = fqdn
        vm_name = "false"

        stat_file_content = ""
        try:
            stat_file_ptr = open(path_stats_files + "/" + stat_file)
            stat_file_content = stat_file_ptr.read()
            stat_file_ptr.close()
        except (OSError, IOError):
            message = "Error while fetching vm stats file"
            logging.warning(str(message + traceback.format_exc()))
            print(message)
            continue

        stat_file_content = stat_file_content.split('\n')
        for stat_file_line in stat_file_content:
            stat_file_line = stat_file_line.split(':')
            if str(stat_file_line[0]).strip() == 'Name':
                vm_name = str(stat_file_line[1]).strip()
                data['name'] = vm_name
            if str(stat_file_line[0]).strip() == 'Used memory':
                vram_used = int(str(stat_file_line[1]).strip().split(' ')[0])
                data['vram_used'] = vram_used
            if str(stat_file_line[0]).strip() == 'CPU(s)':
                cpu = int(str(stat_file_line[1]).strip())
                data['cpu'] = cpu
                host_cpu_allocated += cpu
            if str(stat_file_line[0]).strip() == 'Max memory':
                vram_alloc = int(str(stat_file_line[1]).strip().split(' ')[0])
                data['maxmem'] = vram_alloc
                host_vram_alloc += vram_alloc

        data['post_date'] = now.isoformat()
        data['cluster'] = cluster
        if not vm_name or vram_alloc <= 0 or cpu <= 0 or not cluster:
            message = "Error while fetching vm stats file"
            logging.warning(str(message + traceback.format_exc()))
            print(message)
            continue

        data_json = json.dumps(data)
        vm_name = vm_name.split('.')[0]
        send_to_elk(elk_url + "/" + main_index + "/" + vm_index, data_json)

    host_data['vRAMallocated'] = kib_to_gib(host_vram_alloc)
    host_data['vCPUallocated'] = host_cpu_allocated

    host_data['CPUratio'] = (float(host_cpu_allocated) /
                             (float(pcpu) *
                              float(cpu_overcommit / 100.0))) * 100.0

    host_data['RAMratio'] = (float(host_vram_alloc) /
                             (float(pram_total) *
                              float(ram_overcommit / 100.0))) * 100.0

    host_data['vCPUfree'] = int(float(pcpu) * float(cpu_overcommit / 100.0) -
                                float(host_cpu_allocated))

    host_data['vRAMfree'] = kib_to_gib(float(pram_total) *
                                       float(ram_overcommit / 100.0) -
                                       float(host_vram_alloc)
                                       )

    host_data_json = json.dumps(host_data)
    send_to_elk(elk_url + "/" + main_index + "/" + hv_index, host_data_json)



if __name__ == "__main__":
    main()
