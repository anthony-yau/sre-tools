#!/usr/local/python3/bin/python3
#coding: utf-8
"""
需要安装的依赖包：
pip install kazoo
pip install PyYAML==5.4.1 -U
"""
from kazoo.client import KazooClient

import yaml
import time

# 避免输出'No handlers could be found for logger "kazoo.client"'
import logging
logging.basicConfig(level=50)

"""
配置文件示例：
---
jobs:
  ${kafka_cluster_name}:
    zk_servers: ${kafka zookeeper address}
"""
kafka_zookeeper_config='kafka_broker_numbers.conf'
fp=open(kafka_zookeeper_config, "r")

def get_broker_nums():
    context = fp.read()
    data = yaml.load(context, Loader=yaml.FullLoader)['jobs']
    for k in data:
        setid=k
        zk_servers=data[k]['zk_servers']
        try:
            zk = KazooClient(hosts='{}'.format(zk_servers), timeout=5.0)
            zk.start()
            node = zk.get_children('/brokers/ids')
            num = len(node)
            zk.stop()
            print("kafka_broker_numbers{cluster=\"%s\"} %s" %(setid, str(num)))
        except Exception as e:
            num=0
            print("kafka_broker_numbers{cluster=\"%s\"} %s" %(setid, str(num)))

if __name__ == '__main__':
    get_broker_nums()