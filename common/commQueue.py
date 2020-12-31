#!/usr/bin/python
#-*-coding:utf-8-*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4
# copyright 2020 WShuai, Inc.
# All Rights Reserved.

# @File: commQueue.py
# @Author: WShuai, WShuai, Inc.
# @Time: 2020/07/14 10:28

class QueueHandler(object):
    def __init__(self, **kwargs):
        self.timeout = kwargs['timeout']
        self.queue = kwargs['generic'].Queue(maxsize = kwargs['maxsize'])
        return
    
    def get(self):
        result = None
        if not self.queue.empty():
            result = self.queue.get(block = True, timeout = self.timeout)
        return result
    
    def get_nowait(self):
        result = None
        if not self.queue.empty():
            result = self.queue.get_nowait()
        return result

    def put(self, msg):
        self.queue.put(msg, block = False)
        return

    def qsize(self):
        return self.queue.qsize()
    
    def full(self):
        return self.queue.full()