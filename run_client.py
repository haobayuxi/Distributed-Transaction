#! /usr/bin/env python

import subprocess
import sys
import logging
from logging import Logger

TMP_DIR = '~/'

logging.basicConfig()
logger = logging.getLogger('')


def run_clients(start_client_id, client_num):
    client_id = start_client_id
    process = []
    while client_id < start_client_id + client_num:
        cmd = ["./tapir_client"]
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
    run_clients(0, 2)


if __name__ == "__main__":
    main()
