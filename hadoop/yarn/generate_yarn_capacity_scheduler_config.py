#coding: utf-8

""" 
生成Yarn 队列的(capacity-scheduler.xml)配置文件
"""

import sys
import time
import json
import requests
import parsel
import shutil

from decimal import Decimal
from xml.etree import ElementTree as ET

DEFAULT_PARTITION = "DEFAULT_PARTITION"
DEFAULT_MAX_APPLICATIONS = "10000"


def generate_property_xml(parent, name, value):
    property = ET.SubElement(parent, 'property')
    name_dom = ET.SubElement(property, 'name')
    name_dom.text = name
    value_dom = ET.SubElement(property, 'value')
    value_dom.text = value


#  通过每个队列配置Operator为GE, 实现标签剩余内存的分配
def calculate_partition_remain_memory_mb(partition_info, partition_memory):
    total_memory_mb = 0
    for queue_info in partition_info["queue"]:
        if 'Operator' in queue_info and queue_info["Operator"] == "GE":
            continue
        max_memory_mb = float(queue_info["max_memory_mb"])
        total_memory_mb = total_memory_mb + max_memory_mb
    return float(partition_memory) - total_memory_mb


# 将内存配置值转换成比率值
def generate_capacity_value(queue_info, partition_memory, partition_remain_memory):
    max_memory_mb = queue_info["max_memory_mb"]
    if 'Operator' in queue_info and queue_info["Operator"] == "GE":
        # 如果标签剩余内存低于配置值, 将报错
        if partition_remain_memory < float(max_memory_mb):
            raise Exception("队列 " + queue_info["queue_name"] + " partition_remain_memory:"
                            + str(partition_remain_memory) + "< max_memory_mb:" + max_memory_mb)
        max_memory_mb =str(partition_remain_memory)

    # 如果队列配置内存容量为0, 将转换成比率值
    if float(partition_memory) == 0:
        print(queue_info["queue_name"] + " partition_memory is 0")
        if 'Operator' in queue_info and queue_info["Operator"] == "GE":
            capacity_value = str(100)
        else:
            capacity_value = str(0)
    else:
        # 将队列配置内存跟标签内存相除得出比率值
        capacity_value = str(format(float(max_memory_mb) * 100 / float(partition_memory), '.3f'))
        # 如果比率值为0结尾, 将去除，避免Yarn启动报错
        capacity_value = capacity_value.rstrip('0').strip('.') if '.' in capacity_value else capacity_value
    
    return capacity_value


# 生成default标签的队列配置
def generate_default_lable_xml(queue_info, queue_name, partition_memory, configuration,partition_remain_memory):
    default_capacity_name = "yarn.scheduler.capacity.root." + queue_name + ".capacity"
    default_capacity_value = generate_capacity_value(queue_info, partition_memory, partition_remain_memory)
    generate_property_xml(configuration, default_capacity_name, default_capacity_value)

    default_max_capacity_name = "yarn.scheduler.capacity.root." + queue_name + ".maximum-capacity"
    default_max_capacity_value = str(100)
    generate_property_xml(configuration, default_max_capacity_name, default_max_capacity_value)

    default_capacity_state = "yarn.scheduler.capacity.root." + queue_name + ".state"
    generate_property_xml(configuration, default_capacity_state, "RUNNING")

    accessible_node_labels = "yarn.scheduler.capacity.root." + queue_info[
        "queue_name"] + ".accessible-node-labels"
    accessible_node_labels_value = DEFAULT_PARTITION
    generate_property_xml(configuration, accessible_node_labels, accessible_node_labels_value)

    max_application = "yarn.scheduler.capacity.root." + queue_info[
        "queue_name"] + ".maximum-applications"
    max_application_value = DEFAULT_MAX_APPLICATIONS
    generate_property_xml(configuration, max_application, max_application_value)


# 生成非default标签的队列配置
def generate_lable_xml(queue_info, queue_name, label, partition_memory, configuration, partition_remain_memory):
    default_capacity_name = "yarn.scheduler.capacity.root." + queue_name + ".capacity"
    default_capacity_value = "0"
    generate_property_xml(configuration, default_capacity_name, default_capacity_value)

    default_max_capacity_name = "yarn.scheduler.capacity.root." + queue_name + ".maximum-capacity"
    default_max_capacity_value = "0"
    generate_property_xml(configuration, default_max_capacity_name, default_max_capacity_value)
    
    default_capacity_state = "yarn.scheduler.capacity.root." + queue_name + ".state"
    generate_property_xml(configuration, default_capacity_state, "RUNNING")

    label_capacity_name = "yarn.scheduler.capacity.root." + queue_info[
        "queue_name"] + ".accessible-node-labels." + label + ".capacity"
    
    # 计算容量
    label_capacity_value = generate_capacity_value(queue_info, partition_memory, partition_remain_memory)
    generate_property_xml(configuration, label_capacity_name, label_capacity_value)

    label_max_capacity_name = "yarn.scheduler.capacity.root." + queue_info[
        "queue_name"] + ".accessible-node-labels." + label + ".maximum-capacity"
    label_max_capacity_value = label_capacity_value

    generate_property_xml(configuration, label_max_capacity_name, label_max_capacity_value)

    max_application = "yarn.scheduler.capacity.root." + queue_info[
        "queue_name"] + ".maximum-applications"
    max_application_value = DEFAULT_MAX_APPLICATIONS
    generate_property_xml(configuration, max_application, max_application_value)

    accessible_node_labels = "yarn.scheduler.capacity.root." + queue_info[
        "queue_name"] + ".accessible-node-labels"
    accessible_node_labels_value = label
    generate_property_xml(configuration, accessible_node_labels, accessible_node_labels_value)

    default_node_label_expression = "yarn.scheduler.capacity.root." + queue_info[
        "queue_name"] + ".default-node-label-expression"
    default_node_label_expression_value = label
    generate_property_xml(configuration, default_node_label_expression, default_node_label_expression_value)


# 格式化
def indent(elem, level=0):
    i = "\n" + level * "\t"
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "\t"
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent(elem, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def generate_capacity_xml(capacity_scheduler_file, partition_infos):
    configuration = ET.Element('configuration')
    # 通用配置
    generate_property_xml(configuration, "yarn.scheduler.capacity.maximum-applications", DEFAULT_MAX_APPLICATIONS)
    generate_property_xml(configuration, "yarn.scheduler.capacity.maximum-am-resource-percent", "0.2")
    generate_property_xml(configuration, "yarn.scheduler.capacity.resource-calculator",
                          "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator")
    generate_property_xml(configuration, "yarn.scheduler.capacity.node-locality-delay", "-1")
    generate_property_xml(configuration, "yarn.scheduler.capacity.rack-locality-additional-delay", "-1")
    generate_property_xml(configuration, "yarn.scheduler.capacity.queue-mappings", "")
    generate_property_xml(configuration, "yarn.scheduler.capacity.queue-mappings-override.enable", "false")
    generate_property_xml(configuration, "yarn.scheduler.capacity.application.fail-fast", "false")
    generate_property_xml(configuration, "yarn.scheduler.capacity.workflow-priority-mappings-override.enable", "false")
    
    # 队列配置
    label_names = []
    queue_names = []
    for partition_info in partition_infos:
        label = partition_info["label"]
        partition_memory = partition_info["memory_mb"]
        # 通过标签的内存跟队列配置内存, 计算出标签剩余内存
        partition_remain_memory = calculate_partition_remain_memory_mb(partition_info, partition_memory)
        for queue_info in partition_info["queue"]:
            queue_name = queue_info["queue_name"]
            if label not in label_names:
                label_names.append(label)
            
            # 进行队列检查, 每个队列只能配置一次
            if queue_name in queue_names:
                raise Exception("队列 " + queue_name + " 仅支持配置一次.")
            
            queue_names.append(queue_name)
            
            # 生成队列xml配置
            if label and label != "default":
                generate_lable_xml(queue_info, queue_name, label, partition_memory, configuration,
                                   partition_remain_memory)
            else:
                generate_default_lable_xml(queue_info, queue_name, partition_memory, configuration, 
                                           partition_remain_memory)

    # 生成标签的容量配置
    for lable_name in label_names:
        if lable_name != "default":
            lable_name_capacity = "yarn.scheduler.capacity.root.accessible-node-labels." + lable_name + ".capacity"
            lable_name_capacity_value = "100"
            generate_property_xml(configuration, lable_name_capacity, lable_name_capacity_value)

    # 配置子队列
    root_queue_name = "yarn.scheduler.capacity.root.queues"
    root_queue_value = ','.join(queue_names)
    generate_property_xml(configuration, root_queue_name, root_queue_value)

    tree = ET.ElementTree(configuration)
    root = tree.getroot()
    indent(root)
    tree.write(capacity_scheduler_file)


def get_lable_capacity(host):
    label_memory = {}
    url = 'http://%s/cluster/nodelabels' %host
    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
        }
    try:
        response = requests.get(url=url, headers=headers)
        selector = parsel.Selector(response.text)
    except Exception as e:
        print(e)
        sys.exit(1)

    lis = selector.css('.content tbody tr')
    # 将标签、数量、资源信息输出
    for line in lis:
        info = line.css('td::text').getall()
        if len(info) == 4:
            label_name,_,_,total_resource = info
        else:
            label_name,_,_,_,total_resource = info
        label_name = label_name.strip('\n').strip(' ').strip('\n').strip('<').strip('>')
        total_resource = total_resource.strip('\n').strip(' ').strip('\n')
        memory, _ = total_resource.split(',')
        memory = memory.split(':')[1]
        if memory != '':
            label_memory[label_name] = memory
    return label_memory


def update_queue_info(host, config_file, queue_name, num):
    # 从Yarn UI获取标签的容量
    lable_memory = get_lable_capacity(host)
    
    # 读取队列配置的值(内存大小配置)
    with open(config_file) as f:
        content = f.read()
        old_config_info = json.loads(content)
    
    if len(old_config_info) < 1:
        print("获取队列配置信息错误.")
        sys.exit(1)

    new_config_info = []
    # 更新当前的标签内存容量
    for info in old_config_info['partition_infos']:
        if info['label'] == 'stream':
            info['memory_mb'] = lable_memory['stream']
        elif info['label'] == 'default':
            info['memory_mb'] = lable_memory['DEFAULT_PARTITION']
        elif info['label'] == 'batch':
            info['memory_mb'] = lable_memory['batch']
        new_config_info.append(info)
    old_config_info['partition_infos'] = new_config_info

    # 从队列中获取标签名称
    old_label = queue_name.split('.')[1]
    
    # 更新队列内存信息
    # 默认一台机器100G可用内存
    if old_label == 'stream':
        new_stream_queue_info = []
        for queue in old_config_info['stream_queue_infos']:
            if queue['queue_name'] == queue_name and not 'Operator' in queue:
                # 将机器的内存资源更新到对应队列上
                queue['max_memory_mb'] = str(int(queue['max_memory_mb']) + int(num)*100*1024)
            new_stream_queue_info.append(queue)
        old_config_info['stream_queue_infos'] = new_stream_queue_info

    elif old_label == 'batch':
        new_batch_queue_info = []
        for queue in old_config_info['batch_queue_infos']:
            if queue['queue_name'] == queue_name and not 'Operator' in queue:
                queue['max_memory_mb'] = str(int(queue['max_memory_mb']) + int(num)*100*1024)
                # print(queue['max_memory_mb'])
            new_batch_queue_info.append(queue)
        old_config_info['batch_queue_infos'] = new_batch_queue_info

    # 备份配置文件
    backup_file = config_file + '_' + str(time.strftime("%Y%m%d%H%M%S", time.localtime()))
    shutil.copy(config_file, backup_file)
    with open(config_file,'w') as f:
        f.write(json.dumps(old_config_info, indent=4))

    # 新的配置信息
    new_config_info = []
    for new_queue in old_config_info['partition_infos']:
        new_queue['queue'] = old_config_info[new_queue['queue']]
        new_config_info.append(new_queue)
    return new_config_info

if __name__ == '__main__':
    print("generate yarn queue capacity-scheduler.xml")

    # 更新内存配置文件
    if len(sys.argv[:]) < 3:
        print("提供参数错误.")
        sys.exit(1)
    host = sys.argv[1]
    queue_name = sys.argv[2]
    host_num = int(sys.argv[3])
    config_file = 'yarn_queue_info.json'
    capacity_scheduler_file = "capacity-scheduler.xml"
    new_config_info = update_queue_info(host, config_file, queue_name, host_num)

    # 根据配置文件模板生成新的队列配置文件(capacity-scheduler.xml)
    generate_capacity_xml(capacity_scheduler_file, new_config_info)
    