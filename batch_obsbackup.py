import sys
import os
import json
import re
import datetime
import pyhdfs
from obs import ObsClient
import subprocess
import multiprocessing
#批量备份数据HDFS->OBS

back_list="/home/caic-00213/Obs2Hdfs/back_mask01.txt"
from_path="/cab/mark/caic/vinseg/frontcamera02"
save_path="/data/"+from_path.split("/")[-1]
to_path="obs://zhuyuanhao"+ "/cab/mark/caic/vinseg"  #"/cab/mark/caic/vinseg/""


class input():
    #解析待备份地址列表
    def get_path_list(self,dir_list):
        path_list=[]
        f=open(dir_list,'r')
        path_list=f.readlines()
        print(len(path_list))
        return path_list


class hdfs_side():

    def __init__(self):
        self.host="10.79.131.12"
        self.port="8020"

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
            elem=line.split(" ")[-1].split("/")[-1]
            if(elem!='' and elem!="items"):
                hdfs_list.append(elem)
        return hdfs_list


class obs_side():

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

    path_list=input().get_path_list(back_list)
    for path in path_list:
        path=path.replace("\n",'')
        from_path=path
        save_path="/data/"+from_path.split("/")[-1]
        l=len(from_path.split("/")[-1])
        to_path="obs://zhuyuanhao"+ from_path[0:len(from_path)-l-1]
        os.system("mkdir "+save_path)
        
        path_list_hdfs=hdfs_side().get_file_list()
        path_list_obs=obs_side().get_file_list()
        mv_list=list(set(path_list_hdfs)^set(path_list_obs))
        for path in mv_list:
            hdfs_side().get_file(path)
            if(obs_side().put_file(path)):
        #如果上传成功清空中转文件夹
                os.system("rm -rf "+ save_path +"/"+ path)

