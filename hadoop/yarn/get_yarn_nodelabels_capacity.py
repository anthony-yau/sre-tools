#coding: utf-8

""" 
获取Yarn 标签的容量信息
"""

import requests
import parsel
import sys

host = sys.argv[1]
url = 'http://%s/cluster/nodelabels' %host
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
    }

response = requests.get(url=url, headers=headers)
selector = parsel.Selector(response.text)

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
    memory, vcores = total_resource.split(',')
    memory = memory.split(':')[1]
    vcores = vcores.split(':')[1].strip('>')
    num = line.css('td a::text').get()
    if num:
        num = num.strip('\n').strip(' ')
    elif num is None:
        num = 0

    # 输出信息
    if label_name != '' and num != '':
        #print(type(num))
        print("yarn_lable_nm_count{label_name=\"%s\"} %s" %(label_name, str(num)))
    else:
        print("yarn_lable_nm_count{label_name=\"%s\"} %s" %(label_name, str(0)))
    if label_name != '' and memory != '':
        print("yarn_lable_memory_size{label_name=\"%s\"} %s" %(label_name, str(memory)))
    else:
        print("yarn_lable_memory_size{label_name=\"%s\"} %s" %(label_name, str(0)))
        
    if label_name != '' and vcores != '':
        print("yarn_lable_vcores_count{label_name=\"%s\"} %s" %(label_name, str(vcores)))
    else:
        print("yarn_lable_vcores_count{label_name=\"%s\"} %s" %(label_name, str(0)))
