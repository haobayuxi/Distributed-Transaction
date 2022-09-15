#! /usr/bin/env python
#ClientNum = 900
#RequestPerClient = 100
import pexpect
# read throughput first, then latency

import os
import sys
passwd_key = "hpcgrid3102\n"

t01 = "192.168.50.11"
t03 = "192.168.50.13"
t05 = "192.168.50.15"


def get_data(ip, id):
    cmdline = "scp root@%s:/home/wuhao/yuxi/%sthroughput.data ." % (ip, id)
    try:
        child = pexpect.spawn(cmdline)
        child.expect("password:")
        child.sendline(passwd_key)
        child.expect(pexpect.EOF)
        print("%s get success", ip)
    except Exception as e:
        print("get fail:", e)


def per_server_throughput(id):
    result = []
    file_name = str(id) + "throughput.data"
    f = open(file_name)
    for line in f:
        result.append(float(line.strip('\n')))
    f.close()
    # os.remove(file_name)
    result


def read_throughput_results(type):
    result = 0.0
    result1 = per_server_throughput(0)
    result2 = per_server_throughput(1)
    result3 = per_server_throughput(2)
    for i in range(0, 20):
        #aggregate = result1[i] + result2[i] + result3[i]
        print(result3[i])
    file_name = type + "/throughput"
    result_file = open(file_name, 'a')
    result_file.write(str(result)+'\n')
    result_file.close()


def read_latency_results(RequestPerClient, ClientNum):
    latencys = []
    i = 0
    while i < ClientNum:
        i += 1
        file_name = str(i)+"latency.data"
        f = open(file_name)
        for line in f.readlines(RequestPerClient):
            latencys.append(int(line.strip('\n')))
        f.close()
    latencys.sort()
    # median latency
    file_name = str(ClientNum)
    result_file = open(file_name, 'a')
    latency = latencys[RequestPerClient * ClientNum / 2]
    result_file.write(str(latency))
    result_file.close()


if __name__ == "__main__":
    # type = sys.argv[1]
    servertype = sys.argv[1]
    # if type == "t":
    print("aggregate throughput")
    get_data(t01, 0)
    get_data(t03, 1)
    get_data(t05, 2)
    read_throughput_results(servertype)
