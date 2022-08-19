from operator import eq
from xml.dom import minidom
import mysql.connector
import pymysql
from sqlalchemy import column, create_engine
import csv
import xml.etree.ElementTree as ET
import os
import pandas as pd
from collections import defaultdict
from tabulate import tabulate
import warnings
from sqlalchemy import exc as sa_exc
import pandasql

tree = ET.parse('./connecting.xml', parser = ET.XMLParser(encoding = 'iso-8859-5')) 

def getDataSources(tree,datasource,name):
    datasources={}
    for node in tree.iter(datasource):
        sourceDetails={}
        # print ([i for i in node.iter()])
        for elem in node.iter():
            if not elem.tag==node.tag:
                # print (elem,[i for i in elem.iter()])
                if not next(elem.iter()):
                    sourceDetails[elem.tag]=elem.text
        datasources[sourceDetails[name]]=sourceDetails
    return (datasources)
def parsing(sql):
    sql=" ".join(sql.split())
    # print (sql)
    sql2=sql.lower()
    start=0
    database={}
    all_as=[]
    for i,n in enumerate(sql2.split()):
        if n=="as":
            # print(i)
            all_as.append(i)
    #s=sql2.split()
    s=sql2.split()
    for i in all_as[1:]:

        database[s[i+1]]=s[i-1]
    print(database)
    
    start=0
    
    columns=defaultdict(list)
    for _ in range(sql2.count("select")):
        start=idx1=sql2.find("select",start)+6
        start=idx2=sql2.find("from",start)
        sub_str=sql[idx1:idx2].split(",")
        # print ("sub_str",sub_str)

        for i, substr in enumerate(sub_str):
            if(substr.lower().startswith((" sum", " avg", " count", " max", " min"))):
                sub_str[i] = substr[substr.find("(")+1:substr.find(")")]
        # print("sub_str1",sub_str)

        for i,n in enumerate(sub_str):
            n=n.replace(" ","")
            
            data_model,col=n.split(".")
            columns[database[data_model]].append(col)
    print(columns)
    st = sql.split()
    # print("st", st)
    for i,n in enumerate(st):
        if n=="==":
            before=st[i-1].replace(" ","")
            after=st[i+1].replace(" ","")

            data_model,col=before.split(".")
            columns[database[data_model]].append(col)
            data_model,col=after.split(".")
            columns[database[data_model]].append(col)

            
    print(columns)
    return columns
rdbms=(getDataSources(tree,'rdbms_datasource','dbname'))
csvinfo=(getDataSources(tree,'csv_datasource','csvname'))
xmlinfo=(getDataSources(tree, 'xml_datasource', 'xmlname'))
viewstore = (getDataSources(tree, 'viewstore', 'dbname'))
print(rdbms)
print(csvinfo)
print (xmlinfo)
print(viewstore)

print (parsing("CREATE VIEW v5 as select dc.Cust_id, dc.product_category, sum(fs.Sales) as sum_sales from csv$star as fs inner join ( select pq.product_category, ls.cust_id from sql$marketdb$dim_prod as pq inner join sql$marketdb$fact_sales as ls on pq.prod_id == ls.prod_id ) as dc on dc.Cust_id == fs.Cust_id group by dc.Cust_id, dc.product_category order by sum_sales desc"))