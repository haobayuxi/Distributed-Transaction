#! /usr/bin/env python

import subprocess
import sys
import logging
from logging import Logger

TMP_DIR = '~/'

logging.basicConfig()
logger = logging.getLogger('')


def run_clients():
    client_id = 0
    process = []
    while client_id < 2:
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
    run_clients()


if __name__ == "__main__":
    main()
