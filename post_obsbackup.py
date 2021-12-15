import sys
import os
import json
import re
import datetime
import pyhdfs
from obs import ObsClient
import subprocess
import multiprocessing

from_path="/cab/mark/caic/vinseg/frontcamera02"
save_path="/data/"+from_path.split("/")[-1]
to_path="obs://zhuyuanhao"+ from_path  #"/cab/mark/caic/vinseg/""

class hdfs_side():

    def __init__(self):
        self.host="10.79.131.12"
        self.port="8020"
        #self.from_path="/cab/mark/caic/vinseg/frontcamera02"
        #self.save_path="/data/post_Data"  #本地中转文件夹

    def connect(self):
        client = pyhdfs.HdfsClient(hosts="%s,%s"%(self.host,str(9000)),user_name="root")
        return client

    #下载HDFS数据到本地
    def get_file(self,path):
        hdfs=self.connect()
        try:
            if(path!=""):
                path=from_path+"/"+path.split("/")[-1]
                print("Downloading: %s...."%path)            
                os.system('hadoop fs -get '+path+' '+save_path)           
        except Exception as e:
            print("Download failed:",e)

    #获取目标目录下的全部下级文件夹名称列表
    def get_file_list(self):
        hdfs_list=[]
        r=os.popen("hadoop fs -ls hdfs:"+from_path)
        info=r.readlines()
        for line in info:
            line=line.strip('\r\n')
            elem=line.split(" ")[-1][43:]
            if(elem!=''):
                hdfs_list.append(elem)
        return hdfs_list


class obs_side():

    #def __init__(self):
        #self.save_path="/data/post_Data"   #本地中转文件夹  

    def connect(self):
        obsClient=ObsClient(
            access_key_id='KMTGI5279JNICNJ68Z2D',
            secret_access_key='FVwbD8slRV6jCemryAZ1xf6pClkqw3lXbTKPIIJ0',
            server='100.125.32.189'
        )
        return obsClient

    #上传数据到OBS
    def put_file(self,path):
        obs=self.connect()
        print(obs)
        if(path!=""):
            print(path)
            os.system("obsutil cp -f -u -r -vlength -vmd5 "+save_path+"*"+" "+to_path)
            return 1
           
    #查看OBS的目标桶内已有下级文件夹名称列表
    def get_file_list(self):
        obs=self.connect()
        print(obs)
        obs_list=[]
        r=os.popen("obsutil ls -d "+to_path)
        info=r.readlines()
        for line in info:
            line=line.strip('\r\n')
            if(line[0:3]=="obs"):
                temp=line.split("/")[9]
                if temp in obs_list:
                    pass
                else:
                    obs_list.append(temp)
        return obs_list


if __name__=="__main__":
    #print(save_path)
    os.system("mkdir "+save_path)
    mv_list=[]
    #获取目标HDFS路径下的所有文件夹列表
    path_list_hdfs=hdfs_side().get_file_list()
    #print(path_list_hdfs)
    path_list_obs=obs_side().get_file_list()
    #print(path_list_obs)
    #print(path_list_obs)

    #确定未在OBS上的子文件夹（去掉HDFS和OBS公共部分）
    mv_list=list(set(path_list_hdfs)^set(path_list_obs))

    #print(mv_list)
    #print(len(mv_list))
    #待转移列表mv_list作为输入：下载到本地，上传至OBS
    for path in mv_list:
        hdfs_side().get_file(path)
        if(obs_side().put_file(path)):
        #如果上传成功清空中转文件夹
            #os.system("ls -l "+hdfs_side().save_path)
            os.system("rm -rf "+ save_path +"/"+ path)
        #print(hdfs_side().save_path +"/"+ path)
