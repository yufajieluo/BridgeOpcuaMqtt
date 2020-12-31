#!/usr/bin/python
#-*-coding:utf-8-*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4
# copyright 2020 WShuai, Inc.
# All Rights Reserved.

# @File: commFile.py
# @Author: WShuai, WShuai, Inc.
# @Time: 2020/3/2 10:28

import os
import yaml
import pickle

class FileHandler(object):
    def __init__(self):
        return

    def dumps(self, **kwargs):
        if kwargs['type'] == 'yaml':
            with open(kwargs['file'], 'w', encoding = 'utf-8') as file_handler:
                yaml.dump(kwargs['content'], file_handler)
        elif kwargs['type'] == 'pickle':
            with open(kwargs['file'], 'wb') as file_handler:
                pickle.dump(kwargs['content'], file_handler)
        
        return
    
    def loads(self, **kwargs):
        content = None
        try:
            with open(kwargs['file'], 'rb') as file_handler:
                if kwargs['type'] == 'yaml':
                    content = yaml.safe_load(file_handler)
                elif kwargs['type'] == 'pickle':
                    content = pickle.load(file_handler)
        except:
            pass
        return content