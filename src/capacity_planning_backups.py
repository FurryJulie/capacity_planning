#!/usr/bin/python3

"""
Author : Julie Daligaud <julie.daliugaud@gmail.com>

MIT License

Copyright (c) 2019 Julie Daligaud

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


"""
Script that fetches stats on zfs backup pool and send it
to elastic search.
"""

import os
from subprocess import Popen, PIPE
import datetime
import logging
import traceback
from time import gmtime, strftime
import sys
import json
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


def bytes_to_gib(value):
    """ Take kib and return the value in gib. """

    return int(float(value) / 1024.0 / 1024.0 / 1024.0)


def send_to_elk(url, data_json):
    """
    Send data formated in JSON to the elastic search stack.
    """

    try:
        requests.post(url, data=data_json, timeout=5)
    except RequestException:
        sys.exit("Error while sending data to elasticsearch at " + url)


def main():
    """ Main function. """

    __location__ = os.path.realpath(os.path.join(os.getcwd(),
                                                 os.path.dirname(__file__)))

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

    logfile = conf['logs']
    elk_url = conf['url']
    main_index = conf['indexes']['main']
    backuphost_url = conf['indexes']['backup_hosts']
    datacenter = conf['datacenter']

    if not logfile or not elk_url or not backuphost_url or not datacenter:
        sys.exit("Error while parsing conf file")
    # End parse conf file

    logfile = logfile + ".log"
    logging.basicConfig(filename=logfile, level=logging.DEBUG)
    logging.info(str(strftime("\n\n-----\n" + "%Y-%m-%d %H:%M:%S", gmtime()) +
                     " : Starting capacity planning script."))

    now = datetime.datetime.now()

    # Get name and domain of host (fqdn)
    hostname = call_cmd("hostname")
    domain = call_cmd("hostname -d")
    hostname = hostname.strip()
    fqdn = hostname + "." + str(domain).strip()

    # Get info about backup
    zfs_list = call_cmd("/sbin/zfs list backup -Hp")
    zfs_list = zfs_list.split('\t')
    zfs_list = [_f for _f in zfs_list if _f]

    volume_used = bytes_to_gib(float(zfs_list[1].strip()))
    volume_free = bytes_to_gib(float(zfs_list[2].strip()))
    volume_total = volume_used + volume_free
    volume_ratio = (volume_used * 100.0) / volume_total

    # get compress ratio
    compress_ratio = call_cmd("/sbin/zfs get compressratio backup -Hp")
    compress_ratio = compress_ratio.split('\t')[2]
    compress_ratio = compress_ratio[:-1]

    try:
        compress_ratio = float(compress_ratio)
    except ValueError:
        compress_ratio = 0.0

    # get Logical Used
    logical_used = call_cmd("/sbin/zfs get logicalused backup -Hp")
    logical_used = int(logical_used.split('\t')[2])
    logical_used = bytes_to_gib(logical_used)

    # forge server data
    # These labels are the fileds in the ES index
    host_data = {
        'name':  fqdn,
        'volumeUsed': volume_used,
        'volumeFree': volume_free,
        'volumeTotal': volume_total,
        'volumeRatio': volume_ratio,
        'compressRatio': compress_ratio,
        'volumeLogUsed': int(logical_used),
        'volumeLogFree': int(float(volume_free) * compress_ratio),
        'post_date': now.isoformat(),
        'datacenter': datacenter
    }

    send_to_elk(elk_url + "/" + main_index + "/" + backuphost_url,
                json.dumps(host_data))


if __name__ == "__main__":
    main()
