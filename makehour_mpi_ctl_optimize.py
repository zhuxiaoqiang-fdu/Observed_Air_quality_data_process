#!/public/software/nodes/python361/bin/python3

"""
The purpose of this program is to process the observation data by using multiprocessing
             Written by SQZHU@ACES.FDU on Nov 1st, 2020
             E-mail:19110740012@fudan.edu.cn
"""


import multiprocessing, signal,time,queue
import math
import csv
import numpy as np
import pandas as pd
import os
import sys


def manual_fct(job_queue,result_queue,rootdir_p,list_table,lt_table):
    signal.signal(signal.SIGINT,signal.SIG_IGN)
    while not job_queue.empty():
        try:
            job=job_queue.get(block=False)
            result_queue.put(process(rootdir_p,list_table,lt_table))
        except queue.Empty:
            pass

def process(rootdir_p,list_table,lt_table):
    for ii in range(len(lt_table)):
        sta_name=lt_table[ii]
        print(sta_name)
        fn=len(list_table)
        for i in range(fn):
            path=os.path.join(rootdir_p,list_table[i])
            try:
                read = pd.read_csv(path)
                read.rename(columns={'type':'typee'},inplace=True)
                nn=read[sta_name].values
                hh=read['hour'].values
                hh1=read['hour'].values
                d1=read['date'].values
                hrlen=len(nn)/15
                nnb={}
                seq=range(0,145)
                nnb=nnb.fromkeys(seq,-999.0)
                for k in range(0,24):
                    if k in hh:
                        a=read[(read.hour==k)&(read.typee=='PM2.5')].index.tolist()
                        b=read[(read.hour==k)&(read.typee=='PM10')].index.tolist()
                        c=read[(read.hour==k)&(read.typee=='SO2')].index.tolist()
                        d=read[(read.hour==k)&(read.typee=='NO2')].index.tolist()
                        e=read[(read.hour==k)&(read.typee=='O3')].index.tolist()
                        f=read[(read.hour==k)&(read.typee=='CO')].index.tolist()
                        if nn[a[0]] > 0:
                            nnb[6*k+1]=nn[a[0]]
                        if nn[b[0]] > 0:
                            nnb[6*k+2]=nn[b[0]]
                        if nn[c[0]] > 0:
                            nnb[6*k+3]=nn[c[0]]
                        if nn[d[0]] > 0:
                            nnb[6*k+4]=nn[d[0]]
                        if nn[e[0]] > 0:
                            nnb[6*k+5]=nn[e[0]]
                        if nn[f[0]] > 0:
                            nnb[6*k+6]=nn[f[0]]        


                    with open(sta_name+".PM25.ts"+".txt","a") as f1:
                        dstr=str(d1[1]*100+k)
                        sumpm25str=str(np.round(nnb[k*6+1],3))
                        f1.writelines(dstr + '   ' + sumpm25str + '\n')
                    with open(sta_name+".PM10.ts"+".txt","a") as f2:
                        sumpm10str=str(np.round(nnb[k*6+2],3))
                        f2.writelines(dstr + '   ' + sumpm10str + '\n')
                    with open(sta_name+".SO2.ts"+".txt","a") as f3:
                        sumso2str=str(np.round(nnb[k*6+3],3))
                        f3.writelines(dstr + '   ' + sumso2str + '\n')
                    with open(sta_name+".NO2.ts"+".txt","a") as f4:
                        sumno2str=str(np.round(nnb[k*6+4],3))
                        f4.writelines(dstr + '   ' + sumno2str + '\n')
                    with open(sta_name+".O3.ts"+".txt","a") as f5:
                        sumo3str=str(np.round(nnb[k*6+5],3))
                        f5.writelines(dstr + '   ' + sumo3str + '\n')
                    with open(sta_name+".CO.ts"+".txt","a") as f6:
                        sumcostr=str(np.round(nnb[k*6+6],3))
                        f6.writelines(dstr + '   ' + sumcostr + '\n')

            except:            
                print(sta_name+' error!')


if __name__=='__main__':
    job_queue=multiprocessing.Queue()
    result_queue=multiprocessing.Queue()

    core_num=64
    station=pd.read_csv('/data1/zhusq/long_trend_O3/post_cmaq/03_extract_1layer/36km.xy.China_2019jl.txt',header=None,sep='  ',usecols = [0])
    rootdir='/public/home/data/zhusq/post_cmaq/quanguo_air/station_20200101-20201003/'
    list=os.listdir(rootdir)
    list.sort()

    for i in range(core_num):
        job_queue.put(None)

    workers = []


    tot=len(station)
    gn=math.floor(tot/core_num)
    add=tot-gn*core_num

    for i in range(core_num):
        if i==0:
            j=0

        a=i*gn
        b=(i+1)*gn

        lt_table=station[0][a:b]
        if i<add:
            lt_table.loc[(core_num)*gn+j]=station[0][int((core_num)*gn+j)]
            j=j+1

        lt_table.index=range(0,len(lt_table))
        p=multiprocessing.Process(target=manual_fct,args=(job_queue,result_queue,rootdir,list,lt_table,))
        p.start()
        workers.append(p)
     
    try:
        for worker in workers:
            worker.join()
            print(worker)
    except KeyboardInterrupt:
        print('parent received ctrl-c')
        for worker in workers:
            worker.terminate()
            worker.join()
 
    while not result_queue.empty():
        print(result_queue.get(block=False))









