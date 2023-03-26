# coding: utf-8

"""
帮助文档: https://github.com/kubernetes-client/python/blob/master/kubernetes/README.md
功能：获取节点的容量、Pod的资源配置，实现特定NameSpace的资源配额调整
"""

from kubernetes import client, config
from kubernetes.client.rest import ApiException
import typing
import re
import yaml
import sys

config.load_kube_config()

# 存储集群资源信息
cluster_resource = {"cpu": 0, "memory": 0}

# 存储NameSpace的Pod limits和requests资源
pod_resources = {}

class CollationK8sResource(object):
    def __init__(self) -> None:
        self.k8s_api = client.CoreV1Api()
    
    @classmethod
    def get_cluster_nodes_resources(cls) -> typing.Dict:
        """从Kubernetes集群中获取Nodes的资源容量信息

        Returns:
            Dict: 集群Nodes的总容量
        """
        try:
            ret = cls().k8s_api.list_node()
            for i in ret.items:
                # 节点的容量信息
                capacity = i.status.capacity
                cpus = capacity.get("cpu")
                memorys = capacity.get("memory").strip("Ki")
                
                # 节点的地址信息
                addresses = i.status.addresses
                for addr in addresses:
                    if addr.type == "InternalIP":
                        node_ip = addr.address
                    elif addr.type == "Hostname":
                        node_name = addr.address
                # print(node_ip, node_name, cpus, memorys)
                
                # 转换成 m
                cluster_resource["cpu"] += int(cpus) * 1000
                # 单位是 Ki
                cluster_resource["memory"] += int(memorys)
            return cluster_resource
        except ApiException as e:
            print(e)

    @staticmethod
    def update_limit(namespace, pod_name, resource_type, cpu, memory) -> None:
        """将Pod的limits 和 requests 资源统一单位, 并进行累加

        Args:
            namespace (string): 命名空间
            pod_name (string): Pod名称
            resource_type (string): 资源类型(limits or requests)
            cpu (string): CPU设置值
            memory (string): 内存设置值
        """
        # 内存和CPU单位后缀的正则对象
        memory_suffixes_re = re.compile(r"[a-zA-Z]+")
        cpu_suffixes_re = re.compile(r"[a-zA-Z]$")
    
        # 将内存的单位后缀Ei、Pi、Ti、Gi、Mi、Ki、E、P、T、G、M、k进行替换
        if memory == 0:
            memory = int(memory)
        elif 'Ki' in memory or 'K' in memory:
            memory = int(memory_suffixes_re.sub("", memory))
        elif 'Mi' in memory or 'M' in memory:
            memory = int(memory_suffixes_re.sub("", memory)) * 1024
        elif 'Gi' in memory or 'G' in memory:
            memory = int(memory_suffixes_re.sub("", memory)) * 1024 * 1024
        elif 'm' in memory:
            memory = int(memory_suffixes_re.sub("", memory)) / 1000
        else:
            raise(f"namespace: {namespace} , pod: {pod_name} 的内存获取错误")
        
        # 统一CPU的单位，转换为m
        if cpu == 0:
            cpu = int(cpu)
        elif cpu_suffixes_re.findall(cpu):
            cpu = int(cpu_suffixes_re.sub("", cpu))
        else:
            cpu = int(cpu) * 1000
        
        # 所有Pod的资源进行累加
        if namespace not in pod_resources:
            pod_resources[namespace] = {}
            pod_resources[namespace][resource_type] = {}
            pod_resources[namespace][resource_type]["cpu"] = cpu
            pod_resources[namespace][resource_type]["memory"] = memory
        else:
            if resource_type not in pod_resources[namespace]:
                pod_resources[namespace][resource_type] = {}
                pod_resources[namespace][resource_type]["cpu"] = cpu
                pod_resources[namespace][resource_type]["memory"] = memory
            else:
                pod_resources[namespace][resource_type]["cpu"] += cpu
                pod_resources[namespace][resource_type]["memory"] += memory

    @classmethod
    def get_namespace_pods(cls, namespace) -> typing.Dict:
        """获取某个命名空间的所有Pod资源配置值

        Args:
            namespace (string): 命名空间

        Returns:
            Dict: 命名空间的Pod总资源配置值
        """
        try:
            ret = cls().k8s_api.list_namespaced_pod(namespace)
            for i in ret.items:
                containers = i.spec.containers
                metadata = i.metadata
                pod_name = metadata.name
                
                for container in containers:
                    # print(container.resources)
                    if container.resources:
                        # 获取limits
                        limits = container.resources.limits
                        if limits:
                            limit_cpu = limits.get("cpu", 0)
                            limit_memory = limits.get("memory", 0)
                            cls.update_limit(namespace, pod_name,"limits", limit_cpu, limit_memory)
                        
                        # 获取requests
                        requests = container.resources.requests
                        if requests:
                            request_cpu = requests.get("cpu", 0)
                            request_memory = requests.get("memory", 0)
                            cls.update_limit(namespace, pod_name,"requests", request_cpu, request_memory)
            return pod_resources
        except ApiException as e:
            print(e)


    @classmethod
    def set_namespace_resourcequotas(
            cls,
            namespace: str,
            list_namespace: typing.List = [ "default" ],
            retain_resources: typing.Dict = {
                "cpu": 4000,
                "memory": 8388608
            }
        ) -> None:
        """动态设置命名空间的资源配额

        Args:
            namespace (string): 指定设置资源配额的命名空间
            list_namespace (List): 获取指定命名空间的Pod容量
            retain_resources (Dict, optional): 集群保留资源值. Defaults to { "cpu": 4000, "memory": 8388608 }
        """
        nodes_resources = cls.get_cluster_nodes_resources()
        all_pod_resources = {}
        
        # 获取所有namespace的Pod资源
        for ns in list_namespace:
            pod_resources = cls.get_namespace_pods(ns)
            all_pod_resources.update(pod_resources)
        
        # 总Pod资源
        all_pod_limit_cpu = 0
        all_pod_limit_memory = 0
        
        # 合并所有namespace的Pod资源配置
        for _, resources in all_pod_resources.items():
            all_pod_limit_cpu += resources["limits"]["cpu"] if "limits" in resources else 0
            all_pod_limit_memory += resources["limits"]["memory"] if "limits" in resources else 0
        
        # 将总节点容量 减去 保留容量 和当前设置的Limit值
        cpu_quota = nodes_resources["cpu"] - all_pod_limit_cpu - retain_resources["cpu"]
        memory_quota = nodes_resources["memory"] - all_pod_limit_memory - retain_resources["memory"]
        
        # 进行转换
        cpu_quota = int(cpu_quota / 1000)
        
        # 1Gi-1Ti之间
        if memory_quota >= 1048576 and memory_quota < 1073741824:
            memory_quota = str(int(memory_quota / 1024 / 1024)) + 'Gi'
        elif memory_quota >= 1073741824:
            memory_quota = str(int(memory_quota / 1024 / 1024 / 1024 )) + 'Ti'
        elif memory_quota < 1048576:
            print(f"memory: {memory_quota} is small")
            sys.exit(1)
        
        resource_quota_name = f"{namespace}-resource-quota"
        resourcequotas_yaml = f"""
apiVersion: v1
kind: ResourceQuota
metadata:
  name: {resource_quota_name}
  namespace: {namespace}
spec:
  hard:
    limits.memory: {memory_quota}
        """
        
        resourcequotas_yaml = yaml.load(resourcequotas_yaml, Loader=yaml.FullLoader)
        
        # 列出命名空间下的资源配置
        try:
            ret = cls().k8s_api.list_namespaced_resource_quota(namespace)
            resource_quota_list = []
            
            for item in ret.items:
                resource_quota_list.append(item.metadata.name)
            
            if resource_quota_name in resource_quota_list:
                # 进行更新
                print(f"update resource quota: {resource_quota_name}, limits.memory is: {memory_quota}, limits.cpu is: {cpu_quota}")
                ret = cls().k8s_api.patch_namespaced_resource_quota(
                    name=resource_quota_name,
                    namespace=namespace,
                    body=resourcequotas_yaml
                )
            else:
                # 进行创建
                print(f"create resource quota: {resource_quota_name}, limits.memory is: {memory_quota}, limits.cpu is: {cpu_quota}")
                ret = cls().k8s_api.create_namespaced_resource_quota(
                    namespace=namespace,
                    body=resourcequotas_yaml
                )
        except ApiException as e:
            print(e)
