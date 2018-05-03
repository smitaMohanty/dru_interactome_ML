# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 11:50:38 2016

@author: smita.mohanty
"""

from joblib import Parallel,delayed
import math
import os
import glob
import time
import numpy as np

def opnfil(i):
    det=i.rstrip().rsplit('\t')
    return det
    
def get_correl(v1,v2,corr,i,j):
    #v1=prots[i]
    #v2=prots[j]
    exp1=[v1[k][0] for k in range(4,len(v1))]
    exp2=[v2[k][0] for k in range(4,len(v2))]
    m=set(v1[3][1:]) & set(v2[3][1:])
    M=set(exp1) & set(exp2) 
    corr[(i,j)]=[]
    for l in M:
        x1=exp1.index(l)+4
        x2=exp2.index(l)+4
        t1=[]
        t2=[]
        for k in m:
            id1=v1[3].index(k)
            id2=v2[3].index(k)
            print(i,j,x1,x2,id1,id2,len(v1[x1]),len(v2[x2]))
            if v1[x1][id1] != 'NA' and v2[x2][id2] != 'NA':
                t1.append(float(v1[x1][id1]))
                t2.append(float(v2[x2][id2]))
        if len(t1)>=5 and len(t2)>=5:
           corr[(i,j)].append([np.corrcoef(t1,t2)[0][1],len(t1),len(t2)])
        else:
           corr[(i,j)].append(['NA',len(t1),len(t2)])   
    return corr
       
if __name__ == '__main__':
    prots={}
    corr={}
    now=time.time()
    with Parallel(n_jobs=3) as parallel:
         for i in glob.glob('TMP/*.txt'):
             #print(i)
             #prots[i]=Parallel(n_jobs=3)(delayed(opnfil) (j) for j in open(i))
             #prots[i]=[opnfil (j) for j in open(i)]
             prots[i]=parallel(delayed(opnfil) (j) for j in open(i))
             corr=parallel(delayed(get_correl) (prots[i],prots[k],corr,i,k) for k in prots.keys())   
             print(corr)
             for j in prots.keys():             
                 if i != j:
                    corr=get_correl(j,i,prots,corr)   
    print (time.time()-now)
