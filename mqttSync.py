#!/usr/bin/python
#-*-coding:utf-8-*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4
# copyright 2020 WShuai, Inc.
# All Rights Reserved.

# @File: mqttSync.py.py
# @Author: WShuai, WShuai, Inc.
# @Time: 2020/12/25 9:45

import time
import json
import asyncio
import multiprocessing
import paho.mqtt.client as mqtt

class MqttSync(multiprocessing.Process):
    def __init__(self, **kwargs):
        super(MqttSync, self).__init__()
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.user = kwargs['user']
        self.pswd = kwargs['pswd']
        self.topic_up = kwargs['topic_up']
        self.topic_down = kwargs['topic_down']
        self.queue_up = kwargs['queue_up']
        self.queue_down = kwargs['queue_down']
        self.logger = kwargs['logger']
        self.timeout = 10
        self.client = mqtt.Client()
        return

    def on_connect(self, client, userdata, flags, rc):
        self.logger.info('connect to mqtt server result is {}'.format(rc))
        return

    def on_message(self, client, userdata, msg):
        #self.logger.info('subscribe msg is {}: {}'.format(msg.topic, msg.payload))
        self.queue_down.put(msg.payload)
        return

    def connect(self):
        result = None
        self.logger.info('connect to mqtt server begin...')
        try:
            self.client.username_pw_set(self.user, self.pswd)
            self.client.on_connect = self.on_connect
            result = self.client.connect(self.host, self.port, self.timeout)
            self.logger.info('connect to mqtt server success.')
            result = True
        except Exception as e:
            self.logger.error('connect to mqtt server Exception: {}.'.format(e))
            result = False
        return result

    def publish(self, topic, payload, qos):
        result = None
        #self.logger.info('publish msg to mqtt server begin...')
        try:
            ret = self.client.publish(topic, payload = payload, qos = qos)
            #self.logger.info('publish msg to mqtt server success.{}'.format(ret))
            result = True
        except Exception as e:
            self.logger.error('publish msg {} to mqtt server topic {} Exception: {}.'.format(payload, topic, e))
            result = False
        return result

    def subscribe(self, topic, qos):
        result = None
        self.logger.info('subscribe msg from mqtt server begin...')
        try:
            self.client.subscribe(topic, qos)
            self.client.on_message = self.on_message
            self.logger.info('subscribe msg from mqtt server success.')
            result = True
        except Exception as e:
            self.logger.error('subscribe msg from mqtt server Exception: {}.'.format(e))
            result = False
        return result

    async def sub(self):
        case = 0
        while True:
            if case == 1:
                # connect
                result = self.connect()
                if result:
                    case = 2
                else:
                    case = 1
                    time.sleep(5)
            elif case == 2:
                # sub
                result = self.subscribe(self.topic_down, 0)
                if result:
                    case = 3
                else:
                    case = 4
                    time.sleep(0)
            elif case == 3:
                # test
                self.client.loop()
                await asyncio.sleep(0.1)
            elif case == 4:
                # disconnect
                self.disconnect()
                case = 0
            else:
                # wait
                case = 1
                time.sleep(5)
        return
    
    async def pub(self):
        while True:
            msg = self.queue_up.get()
            if msg:
                self.logger.debug('from opc and pub mqtt msg is {}'.format(msg))
                #topic = '{}/{}'.format(self.topic_up, msg[0])
                #payload = ','.join([str(item) for item in msg[1]]) if isinstance(msg[1], list) else msg[1]
                topic = '{}/{}'.format(self.topic_up, msg['id'])
                payload = json.dumps(msg)
                self.publish(topic, payload, 0)
            else:
                await asyncio.sleep(0.1)
        return

    def run(self):
        loop = asyncio.get_event_loop()
        loop_tasks = []
        loop_tasks.append(
            loop.create_task(self.sub())
        )
        loop_tasks.append(
            loop.create_task(self.pub())
        )
        loop.run_until_complete(
            asyncio.wait(loop_tasks)
        )
        return