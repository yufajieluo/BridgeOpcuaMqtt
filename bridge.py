#!/usr/bin/python
#-*-coding:utf-8-*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4
# copyright 2020 WShuai, Inc.
# All Rights Reserved.

# @File: bridge.py
# @Author: WShuai, WShuai, Inc.
# @Time: 2020/12/27 11:51

import os
import time
import multiprocessing

from router import Router
from opcSync import OpcSync
from mqttSync import MqttSync

from common.commLog import LogHandler
from common.commFile import FileHandler
from common.commQueue import QueueHandler

class Bridge(object):
    def __init__(self, **kwargs):
        self.service_name = kwargs['service_name']
        self.config_file = kwargs['config_file']
        return

    def process(self):
        # init config with yaml
        file_handler = FileHandler()
        configs = file_handler.loads(file = self.config_file, type = 'yaml')
        print('read config is {0}'.format(configs))

        # init log
        logger_handler = LogHandler(configs['logging'])
        logger = logger_handler.register_rotate(self.service_name)
        logger.info('service configs is {0}'.format(configs))

        # init queue
        manager = multiprocessing.Manager()
        queue_up = QueueHandler(
            generic = manager,
            maxsize = configs['queue']['size'],
            timeout = configs['queue']['timeout']
        )
        queue_down = QueueHandler(
            generic = manager,
            maxsize = configs['queue']['size'],
            timeout = configs['queue']['timeout']
        )

        sub_handler_map = {}
        opc_sync = OpcSync(
            url = configs['opc']['url'],
            reserve_device = configs['opc']['reserve_device'],
            reserve_tag = configs['opc']['reserve_tag'],
            queue_up = queue_up,
            queue_down = queue_down,
            logger = logger_handler.register_rotate('sub-opc')
        )

        mqtt_sync = MqttSync(
            host = configs['mqtt']['host'],
            port = configs['mqtt']['port'],
            user = configs['mqtt']['user'],
            pswd = configs['mqtt']['pswd'],
            topic_up = configs['mqtt']['topic_up'],
            topic_down = configs['mqtt']['topic_down'],
            queue_up = queue_up,
            queue_down = queue_down,
            logger = logger_handler.register_rotate('sub-mqtt')
        )

        processes = [opc_sync, mqtt_sync]
        for process in processes:
            process.start()

        while True:
            logger.debug('this is {} main process {}'.format(self.service_name, os.getpid()))
            time.sleep(60)
        
        return


