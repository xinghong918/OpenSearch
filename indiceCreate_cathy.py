#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 文件名：indiceCreate.py
import sys
import base64
import time
import http.client
import json
## 源集群host。
oldClusterHost = "localhost:9200"
## 源集群用户名，可为空
oldClusterUserName = "user"
## 源集群密码，可为空
oldClusterPassword = "psw"
## 源集群SSL是否启用，True为https，False为http
oldClusterSSLEnabled = True

## 目标集群host
newClusterHost = "amaaaaa....opensearch.ap-chuncheon-1.oci.oraclecloud.com:9201"
## 目标集群用户名
newClusterUser = "user"
## 目标集群密码
newClusterPassword = "user"
## 目标集群SSL是否启用，True为https，False为http
newClusterSSLEnabled = True

#目标中迁移时的副本数默认重置为零
DEFAULT_REPLICAS = 0

def httpRequest(method, host, endpoint, params="", username="", password="", sslEnabled=False):
    # conn = http.client.HTTPConnection(host)
    if (False == sslEnabled) :
        conn = http.client.HTTPConnection(host)
    else:
        conn = http.client.HTTPSConnection(host)

    headers = {}
    if (username != "") :
        'Hello {name}, your age is {age} !'.format(name = 'Tom', age = '20')
        # base64string = base64.encodestring('{username}:{password}'.format(username = username, password = password)).replace('\n', '')
        base64string = base64.b64encode(('{username}:{password}'.format(username=username, password=password)).encode()).decode().replace('\n', '')
        headers["Authorization"] = "Basic %s" % base64string;
    if "GET" == method:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        conn.request(method=method, url=endpoint, headers=headers)
    else :
        headers["Content-Type"] = "application/json"
        conn.request(method=method, url=endpoint, body=params, headers=headers)
    response = conn.getresponse()
    res = response.read()
    return res.decode()
def httpGet(host, endpoint, username="", password="", sslEnabled = False):
    return httpRequest("GET", host, endpoint, "", username, password, sslEnabled)
def httpPost(host, endpoint, params, username="", password="", sslEnabled = False):
    return httpRequest("POST", host, endpoint, params, username, password, sslEnabled)
def httpPut(host, endpoint, params, username="", password="", sslEnabled = False):
    return httpRequest("PUT", host, endpoint, params, username, password, sslEnabled)
def getIndices(host, username="", password="", sslEnabled = False):
    endpoint = "/_cat/indices"
    indicesResult = httpGet(host, endpoint, username, password, sslEnabled)
    indicesList = indicesResult.split("\n")
    indexList = []
    for indices in indicesList:
        if (indices.find("open") > 0):
            indexList.append(indices.split()[2])
    return indexList
def getSettings(index, host, username="", password="", sslEnabled = False):
    endpoint = "/" + index + "/_settings"
    indexSettings = httpGet(host, endpoint, username, password, sslEnabled)
    print (index + "  原始settings如下：\n" + indexSettings)
    settingsDict = json.loads(indexSettings)
    ## 分片数默认和源集群索引保持一致。
    number_of_shards = settingsDict[index]["settings"]["index"]["number_of_shards"]
    ## 副本数默认为0。
    # number_of_replicas = DEFAULT_REPLICAS
    # number_of_replicas = settingsDict[index]["settings"]["index"]["number_of_replicas"]

    settingsDict[index]["settings"]["index"]["number_of_replicas"] = DEFAULT_REPLICAS
    del settingsDict[index]["settings"]["index"]["provided_name"]
    del settingsDict[index]["settings"]["index"]["uuid"]
    del settingsDict[index]["settings"]["index"]["creation_date"]
    del settingsDict[index]["settings"]["index"]["version"]
    settings = json.dumps(settingsDict[index]["settings"]["index"])
    newSetting = "\"settings\":" + settings
    return newSetting
def getMapping(index, host, username="", password="", sslEnabled = False):
    endpoint = "/" + index + "/_mapping"
    indexMapping = httpGet(host, endpoint, username, password, sslEnabled)
    print (index + " 原始mapping如下：\n" + indexMapping)
    mappingDict = json.loads(indexMapping)
    mappings = json.dumps(mappingDict[index]["mappings"])
    newMapping = "\"mappings\" : " + mappings
    return newMapping
def createIndexStatement(oldIndexName):
    settingStr = getSettings(oldIndexName, oldClusterHost, oldClusterUserName, oldClusterPassword, oldClusterSSLEnabled)
    mappingStr = getMapping(oldIndexName, oldClusterHost, oldClusterUserName, oldClusterPassword, oldClusterSSLEnabled)
    createstatement = "{\n" + str(settingStr) + ",\n" + str(mappingStr) + "\n}"
    return createstatement
def createIndex(oldIndexName, newIndexName=""):
    if (newIndexName == "") :
        newIndexName = oldIndexName
    createstatement = createIndexStatement(oldIndexName)
    print ("新索引 " + newIndexName + " 的setting和mapping如下：\n" + createstatement)
    endpoint = "/" + newIndexName
    createResult = httpPut(newClusterHost, endpoint, createstatement, newClusterUser, newClusterPassword, newClusterSSLEnabled)
    print ("新索引 " + newIndexName + " 创建结果：" + createResult)


## main
indexList = getIndices(oldClusterHost, oldClusterUserName, oldClusterPassword, oldClusterSSLEnabled)
systemIndex = []
for index in indexList:
    if (index.startswith(".")):
        systemIndex.append(index)
    else :
        createIndex(index, index)
if (len(systemIndex) > 0) :
    for index in systemIndex:
        print (index + " 或许是系统索引，不会重新创建，如有需要，请单独处理～")


