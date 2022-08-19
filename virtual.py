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
view_def_dict = {}

def getDataSources(tree,datasource,name):
    datasources={}
    for node in tree.iter(datasource):
        sourceDetails={}
        for elem in node.iter():
            if not elem.tag==node.tag:
                if not next(elem.iter()):
                    sourceDetails[elem.tag]=elem.text
        datasources[sourceDetails[name]]=sourceDetails
    return (datasources)
    

rdbms=(getDataSources(tree,'rdbms_datasource','dbname'))
csvinfo=(getDataSources(tree,'csv_datasource','csvname'))
viewstore = (getDataSources(tree, 'viewstore', 'dbname'))

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
    # print(database)
    
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
    
    st = sql.split()
    # print("st", st)
    for i,n in enumerate(st):
        if n=="==":
            before=st[i-1].replace(" ","")
            after=st[i+1].replace(" ","")
            # print("hello")
            # print(before)
            # print(after)
            data_model,col=before.split(".")
            columns[database[data_model]].append(col)
            data_model,col=after.split(".")
            columns[database[data_model]].append(col)

            
    # print(columns)
    return columns

def addView(view_definition_query):
    viewname = view_definition_query.split()[2]
    view_def_dict[viewname] = view_definition_query


def generateDataFrames(columns, rdbms, csvinfo):
    df_list = {}
    # print("in gendf", columns)
    for i in columns:
        columns[i]=list(set(columns[i]))
    for key, value in columns.items():
        
        if(key.startswith("sql")):
            dbType, dbname, tablename = key.split("$")
            mydb = mysql.connector.connect(
                    host=rdbms[dbname]["location"],
                    user=rdbms[dbname]["user-name"],
                    password=rdbms[dbname]["password"],
                    database=dbname
                    )
            

            column = ",".join(value)
            query = "SELECT {} FROM {}".format(column, tablename)
            # print(query)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=sa_exc.SAWarning)
    # code here...
                rdbms_pd = pd.read_sql(query, mydb)

            key = key.replace("$", "_")
            df_list[key] = rdbms_pd
            # print(rdbms_pd)

        elif(key.startswith("csv")):
            dbType, dbname  = key.split("$")
            # print (csvinfo[dbname]['csv_loc'],value)
            csv_pd = pd.read_csv(csvinfo[dbname]['csv_loc'], delimiter="\t", usecols=value)
            key = key.replace("$", "_")
            df_list[key] = csv_pd
            # print(csv_pd)
    return df_list

def createView(view_definition_query = None):
    columns = parsing(view_definition_query)
    df_list = generateDataFrames(columns, rdbms, csvinfo)
    # print(df_list)
    view_definition_query = view_definition_query.replace("$", "_")
    view_definition_query = view_definition_query.replace("==", "=")
    view_definition_query = view_definition_query+";"
    view_definition_query_list = view_definition_query.split(" ")
    view_definition_query_to_run = " ".join(view_definition_query_list[4:])
    # print(view_definition_query_to_run)

    # print(sql_marketdb_fact_sales)
    sqlenv = lambda q: pandasql.sqldf(q, df_list)

    req_df = sqlenv(view_definition_query_to_run)
    return req_df, df_list

def runQuery(reqdf, df_list, sql, viewname):

    df_list["reqdf"] = reqdf
    sqlenv = lambda q: pandasql.sqldf(q, df_list)
    

    sql = sql.replace(viewname, "reqdf")
    sql = sql+";"
    output = sqlenv(sql)
    # print(output)
    return output



#---------------------------------------TESTBENCH FOR VIRTUAL FUNCTIONS-----------------------------

# def main():
#     sql1="CREATE VIEW testing1 as select dc.Cust_id, dc.product_category, sum(fs.Sales) as sum_sales from csv$star as fs inner join ( select pq.product_category, ls.cust_id from sql$marketdb$dim_prod as pq inner join sql$marketdb$fact_sales as ls on pq.prod_id == ls.prod_id ) as dc on dc.Cust_id == fs.Cust_id group by dc.Cust_id, dc.product_category"
#     query1 = "SELECT * FROM testing1 WHERE sum_sales < 5000 limit 10"

#     addView(sql1)
#     reqdf,df_list = createView(sql1)
#     output = runQuery(reqdf, df_list, query1, sql1.split()[2])
#     print(output)

# main()


