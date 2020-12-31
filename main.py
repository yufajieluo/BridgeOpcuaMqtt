#!/usr/bin/python
#-*-coding:utf-8-*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4
# copyright 2020 WShuai, Inc.
# All Rights Reserved.

# @File: main.py
# @Author: WShuai, WShuai, Inc.
# @Time: 2020/12/21 10:47

import os
import sys
import argparse


from bridge import Bridge

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', help = 'service name', type = str, required = True)
    parser.add_argument('--conf', help = 'config file path', type = str, required = True)
    args = parser.parse_args()

    if os.path.isfile(args.conf):
        bridge = Bridge(service_name = args.name, config_file = args.conf)
        bridge.process()
    else:
        print('config file {} not exist.'.format(args.conf))
    sys.exit(0)