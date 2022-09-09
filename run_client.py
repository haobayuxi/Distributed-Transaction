#! /usr/bin/env python
import os
import subprocess
import sys
import logging
from time import sleep
import pexpect
from logging import Logger
from aggregate_result import read_latency_results, read_throughput_results
from ruamel import yaml


TMP_DIR = '~/'

IP = "192.168.50.12"

logging.basicConfig()
logger = logging.getLogger('')

passwd_key = "hpcgrid3102\n"


def send_result(ip, filename):
    cmdline = "scp %s root@%s:/home/wuhao/yuxi_data" % (filename, ip)
    try:
        child = pexpect.spawn(cmdline)
        child.expect("password:")
        child.sendline(passwd_key)
        child.expect(pexpect.EOF)
        print("%s send success", ip)
    except Exception as e:
        print("send fail:", e)


def read_local_throughput_results(startid, ClientNum):
    result = 0.0
    result_file_name = str(startid)+"throughput"
    result_file = open(result_file_name, 'w')
    result_latency_file_name = str(startid)+"latency"
    result_latency_file = open(result_latency_file_name, 'w')
    i = startid
    while i < ClientNum + startid:
        file_name = str(i)+"throughput.data"
        latency_name = str(i)+"latency.data"
        f = open(file_name)
        line = f.readline()
        result += float(line.strip('\n'))
        f.close()
        latency_file = open(latency_name)
        latency = latency_file.readline()
        result_latency_file.write(str(latency))
        latency_file.close()
        os.remove(file_name)
        os.remove(latency_name)
        i += 1
    result_file.write(str(result)+"\n")
    result_file.close()
    result_latency_file.close()


def increase_conflict():
    content = []
    with open('config.yml', "r") as f:
        content = yaml.load(f, Loader=yaml.RoundTripLoader)
        x = content['conflict']
        if x == 100:
            content['conflict'] = 0
        else:
            content['conflict'] = x + 25
    with open('config.yml', "w") as f:
        yaml.dump(content, f, Dumper=yaml.RoundTripDumper)


def increase_zipf():
    content = []
    with open('config.yml', "r") as f:
        content = yaml.load(f, Loader=yaml.RoundTripLoader)
        x = content['zipf']
        if x >= 0.95:
            content['zipf'] = 0.5
        else:
            content['zipf'] = x + 0.1
    with open('config.yml', "w") as f:
        yaml.dump(content, f, Dumper=yaml.RoundTripDumper)


def run_clients(start_client_id, client_num):
    client_id = start_client_id
    process = []
    while client_id < start_client_id + client_num:
        cmd = ["./dast_client " + str(client_id)]
 #       cmd.extend([client_id])
        logger.info("running client %d", client_id)
        p = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE)
        process.append(p)
        client_id += 1
    for p in process:
        p.wait()


def main():
    logger.setLevel(logging.DEBUG)
    logger.info("start")
    startid = 0
    num = 300
    run_clients(startid, num)
    read_local_throughput_results(startid, num)
    send_result(IP, str(startid)+"throughput")
    send_result(IP, str(startid)+"latency")
    file_name = str(startid)+"throughput"
    latency_file_name = str(startid)+"latency"
    os.remove(file_name)
    os.remove(latency_file_name)
    # vim config
    increase_zipf()
    logger.info("done")
    # os.path.isfile(fname)


if __name__ == "__main__":
    main()
