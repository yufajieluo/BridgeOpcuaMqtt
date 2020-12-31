#!/usr/bin/python
#-*-coding:utf-8-*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4
# copyright 2020 WShuai, Inc.
# All Rights Reserved.

# @File: opcSync.py
# @Author: WShuai, WShuai, Inc.
# @Time: 2020/12/21 10:47

import time
import json
import opcua
import queue
import asyncio
import multiprocessing

class OpcSync(multiprocessing.Process):
    def __init__(self, **kwargs):
        super(OpcSync, self).__init__()
        self.client = opcua.Client(url = kwargs['url'])
        self.logger = kwargs['logger']
        self.queue_up = kwargs['queue_up']
        self.queue_down = kwargs['queue_down']
        self.reserve_device = kwargs['reserve_device']
        self.reserve_tag = kwargs['reserve_tag']
        self.sub_handler_map = {}
        self.node_data_type_map = {}
        self.queue_data_type_map = queue.Queue()
        self.queue_event = []
        self.queue_status = []
        self.sub_handler = SubHandler(
            self.queue_up,
            self.queue_event,
            self.queue_status,
            self.queue_data_type_map
        )
        self.subscription = None
        
        self.channels = [
            'channel_test'
        ]
        return

    def connect(self):
        result = None
        self.logger.info('connect to opc-ua server begin...')
        try:
            self.client.connect()
            self.client.load_type_definitions()
            self.logger.info('connect to opc-ua server success.')
            result = True
        except Exception as e:
            self.logger.error('connect to opc-ua server Exception: {}.'.format(e))
            result = False
        return result

    def subscribe(self):
        self.logger.info('subscribe nodes and events beging...')
        result = None
        try:
            self.subscription = self.client.create_subscription(500, self.sub_handler)
            for channel in self.channels:
                children_device = self.client.get_node('ns=2;s={}'.format(channel)).get_children()
                for child_device in children_device:
                    if child_device.__str__().split('.')[-1] not in self.reserve_device:
                        children_tags = child_device.get_children()
                        for child_tag in children_tags:
                            if child_tag.__str__().split('.')[-1] not in self.reserve_tag:
                                handler = self.subscription.subscribe_data_change(child_tag)
                                #self.subscription.subscribe_events(child_tag)
                                self.sub_handler_map[child_tag.__str__()] = handler
                                self.node_data_type_map[child_tag.__str__()] = str(child_tag.get_data_type_as_variant_type())
            self.queue_data_type_map.put(self.node_data_type_map)
            self.logger.info('subscribe nodes and events success.')
            result = True
        except Exception as e:
            self.logger.error('subscribe nodes and events Exception: {}.'.format(e))
            result = False
        return result
    
    def publish(self, node_id, type, value):
        #self.logger.info('publish node value beging...')
        result = None
        try:
            node = self.client.get_node(node_id)
            node.set_value(opcua.ua.DataValue(opcua.ua.Variant(value, eval('opcua.ua.{}'.format(type)))))
            #self.logger.info('publish node value success.')
        except Exception as e:
            self.logger.error('publish node {} value {} Exception: {}'.format(node_id, value, e))
        return

    def disconnect(self):
        self.logger.info('unsubscribe from opc-ua servser begin...')
        try:
            for handler in self.sub_handler_map.values():
                self.subscription.unsubscribe(handler)
            self.subscription.delete()
            self.logger.info('unsubscribe from opc-ua servser success.')
        except Exception as e:
            self.logger.error('unsubscribe from opc-ua servser Exception: {}'.format(e))
            self.subscription = None
            self.sub_handler_map = {}

        self.logger.info('disconnect from opc-ua servser begin...')
        try:
            self.client.disconnect()
        except Exception as e:
            self.logger.error('disconnect from opc-ua servser Exception: {}'.format(e))
        return

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
                result = self.subscribe()
                if result:
                    case = 3
                else:
                    case = 4
            elif case == 3:
                # test
                #self.logger.debug('======data queue length is {}'.format(self.queue_up.qsize()))
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
            msg = self.queue_down.get_nowait()
            if msg:
                self.logger.debug('from mqtt and pub opc msg is {}'.format(msg))
                msg_json = json.loads(msg)
                self.publish(msg_json['id'], msg_json['type'], msg_json['value'])
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

class SubHandler(object):
    def __init__(self, queue_data, queue_event, queue_status, queue_data_type_map):
        self.queue_data = queue_data
        self.queue_event = queue_event
        self.queue_status = queue_status
        self.queue_data_type_map = queue_data_type_map
        self.node_data_type_map = {}
        return

    def datachange_notification(self, node, val, data):
        if not self.queue_data_type_map.empty():
            self.node_data_type_map = self.queue_data_type_map.get_nowait()

        self.queue_data.put(
            {
                'id': node.__str__(),
                'type': self.node_data_type_map[node.__str__()],
                'value': val
            }
        )
        #print('data change event: node is {}, value is {}'.format(node, val))

    def event_notification(self, event):
        self.queue_event.append(event.get_event_props_as_fields_dict())
        print('event change event: {}, {}'.format(event, event.get_event_props_as_fields_dict()))

    def status_change_notification(self, status):
        self.queue_status.append(status)
        print('status change event: {}'.format(status))
