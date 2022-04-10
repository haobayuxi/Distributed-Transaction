#! /usr/bin/env python

from http import client
import subprocess
import sys
import logging
from logging import Logger

TMP_DIR = '~/'

logger = logging.getLogger('')


def run_clients():
    client_id = 0
    while client_id < 2:
        cmd = ["./tapir_client"]
        Logger.info("running client %d", client_id)
        res = subprocess.call(cmd)
        client_id += 1


def main():
    logger.setLevel(logging.DEBUG)
    logger.info("start")
    run_clients()


if __name__ == "__main__":
    main()
