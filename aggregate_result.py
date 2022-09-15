#! /usr/bin/env python
#ClientNum = 900
#RequestPerClient = 100
import pexpect
# read throughput first, then latency

import os
import sys
passwd_key = "hpcgrid3102\n"


def get_data(ip):
    cmdline = "scp root@%s:/home/wuhao/yuxi_data/*data ." % ip
    try:
        child = pexpect.spawn(cmdline)
        child.expect("password:")
        child.sendline(passwd_key)
        child.expect(pexpect.EOF)
        print("%s get success", ip)
    except Exception as e:
        print("get fail:", e)


def read_throughput_results(type):
    result = 0.0
    for file_name in os.listdir("."):
        if "throughput" in file_name:
            f = open(file_name)
            line = f.readline()
            print(float(line.strip('\n')))
            result += float(line.strip('\n'))
            print(result)
            f.close()
            os.remove(file_name)
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
    get_data("t01")
    read_throughput_results(servertype)
