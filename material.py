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
        for elem in node.iter():
            if not elem.tag==node.tag:
                if not next(elem.iter()):
                    sourceDetails[elem.tag]=elem.text
        datasources[sourceDetails[name]]=sourceDetails
    return (datasources)

rdbms=(getDataSources(tree,'rdbms_datasource','dbname'))
csvinfo=(getDataSources(tree,'csv_datasource','csvname'))
viewstore = (getDataSources(tree, 'viewstore', 'dbname'))

print(rdbms)
print(csvinfo)
print(viewstore)
#print(viewstore)


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



def generateDataFrames(columns, rdbms, csvinfo, sql):
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

    uploadDataFrames(df_list,sql)

def uploadDataFrames(df_list, sql):

    
    '''user = viewstore['views']["user-name"]
    passw = viewstore['views']["password"]
    host = viewstore['views']["location"] # either localhost or ip e.g. '172.17.0.2' or hostname address 
    port = 3306 
    database = 'views'

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)


        viewsdb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database , echo=False)

        for key, value in df_list.items():
            # print(key)
            # print(value)
            # key = key.replace(".", "_")
            # print(key)
            value.to_sql(key, viewsdb)

        joinDataFrames(sql, df_list.keys())'''
    print("printing df_list", df_list)

    view_definition_query = sql.replace("$", "_")
    view_definition_query = view_definition_query.replace("==", "=")
    view_definition_query = view_definition_query+";"
    view_definition_query_list = view_definition_query.split(" ")
    view_name = view_definition_query_list[2] + "_view"
    print(view_name)
    view_definition_query_to_run = " ".join(view_definition_query_list[4:])
    
    sqlenv = lambda q: pandasql.sqldf(q, df_list)

    req_df = sqlenv(view_definition_query_to_run)
    #df_list["req_df"] = req_df
    print(req_df)

    user = viewstore['views']["user-name"]
    passw = viewstore['views']["password"]
    host = viewstore['views']["location"] # either localhost or ip e.g. '172.17.0.2' or hostname address 
    port = 3306 
    database = 'views'
    print(user, passw, host)

    viewsdb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database , echo=False)

    
    req_df.to_sql(view_name, viewsdb)

        
def joinDataFrames(sql, df_list):

    viewsdb = mysql.connector.connect(
                    host=viewstore['views']["location"],
                    user=viewstore['views']["user-name"],
                    password=viewstore['views']["password"],
                    database='views'
                    )

    sql = sql.replace("==", "=")
    cursor = viewsdb.cursor()
    print(sql)
    cursor.execute(sql)
    viewname = sql.split()[2]

    query = "CREATE TABLE {}_view AS (SELECT * FROM {})".format(viewname, viewname)
    cursor.execute(query)

    query = "DROP VIEW {}".format(viewname)
    cursor.execute(query)
    for key in df_list:
        new_query = "DROP TABLE {}".format(key)
        cursor.execute(new_query)


def getView(viewname):
    viewsdb = mysql.connector.connect(
                    host=viewstore['views']["location"],
                    user=viewstore['views']["user-name"],
                    password=viewstore['views']["password"],
                    database='views'
                    )
    
    query = "SELECT * FROM {}_view".format(viewname)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)
    # code here...
        req_view = pd.read_sql(query, viewsdb)
    return req_view

def QueryView(sql):
    viewsdb = mysql.connector.connect(
                    host=viewstore['views']["location"],
                    user=viewstore['views']["user-name"],
                    password=viewstore['views']["password"],
                    database='views'
                    )
    # cursor = viewsdb.cursor()
    # cursor.execute(sql)
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)
    # code here...
        queried_db = pd.read_sql(sql, viewsdb)
    # print(queried_db)
    return queried_db
    

def listViewNames():

    viewsdb = mysql.connector.connect(
                    host=viewstore['views']["location"],
                    user=viewstore['views']["user-name"],
                    password=viewstore['views']["password"],
                    database='views'
                    )

    cursor = viewsdb.cursor()
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.tables WHERE TABLE_NAME LIKE '%_view' AND table_schema = 'views'"
    cursor.execute(query)
    tables_list = []
    for table in [tables[0] for tables in cursor.fetchall()]:
        # print(table)
        tables_list.append(table)
    return tables_list

def dropView(viewname):
	
	viewsdb = mysql.connector.connect(
                    host=viewstore['views']["location"],
                    user=viewstore['views']["user-name"],
                    password=viewstore['views']["password"],
                    database='views'
                    )
	cursor = viewsdb.cursor()
	viewname = viewname+"_view"
	new_query = "DROP TABLE {}".format(viewname)
	cursor.execute(new_query)
        

#-----------------------------------TESTBENCH FOR CHECKING THE MATERIAL VIEW GENERATION--------------------------

def app():
    print("Select whether you wish to:")
    print("1) Query an Existing View")
    print("2) Create a New View")
    option = int(input())

    if(option == 2):
        print("Enter your View Definition:")
        sql = input()
        viewname = sql.split()[2]
        columns = parsing(sql=sql)
        generateDataFrames(columns, rdbms, csvinfo, sql)
        req_view = getView(viewname)
        print(tabulate(req_view, headers='keys', tablefmt='psql'))
        print("Type yes if you wish to query the view.")
        isQuery = input().lower()
        if(isQuery == "yes"):
            print("In query mode, please enter break to exit.")

            while(1):
                print("Enter Query to Run or break to Exit")
                query = input()
                
                if(query.lower() == "break"):
                    break
                else:
                    query = query.replace(viewname, viewname+"_view")
                    print(query)
                    queried_view = QueryView(query)
                    print(tabulate(queried_view, headers='keys', tablefmt='psql'))
                    print("-------Query Done-------")
        else:
            print("-------View has been stored-------")
        print("-------View has been stored-------")
    if(option == 1):
        while(1):

            print("Enter view name to be loaded or quit to exit")
            viewname = input()
            
            if(viewname == "quit"):
                break
            else:
                viewlist = listViewNames()
                viewnameexists = viewname+"_view"
                if(viewnameexists in viewlist):
                    req_view = getView(viewname)
                    print(tabulate(req_view, headers='keys', tablefmt='psql'))

                    print("Type yes if you wish to query the view.")
                    isQuery = input().lower()
                    if(isQuery == "yes"):
                        print("In query mode, please enter break to exit.")

                        while(1):
                            print("Enter Query to Run or break to Exit")
                            query = input()
                            if(query.lower() == "break"):
                                break
                            else:
                                query = query.replace(viewname, viewname+"_view")
                                print(query)
                                queried_view = QueryView(query)
                                print(tabulate(queried_view, headers='keys', tablefmt='psql'))
                                print("-------Query Done-------")
                    else:
                        print("-------Will Be Taken Back to View Loading Menu-------")




# sql = input()
# columns = parsing(sql)
# generateDataFrames(columns, rdbms, csvinfo, sql)   
# queried_pd = QueryView("SELECT cust_id,sum_sales from connect_view where sum_sales > 1000.0 order by sum_sales")
# tables_list = listViewNames()
# joinDataFrames(sql)















































# CREATE View location2 AS SELECT p.empId, p.name, q.Role FROM sql_qwe_employee as p INNER JOIN csv_employee as q ON p.empId == q.empId
# CREATE VIEW demo as select dc.Cust_id, dc.product_category, sum(fs.Sales) as sum_sales from csv$star as fs inner join ( select pq.product_category, ls.cust_id from sql$marketdb$dim_prod as pq inner join sql$marketdb$fact_sales as ls on pq.prod_id == ls.prod_id ) as dc on dc.Cust_id == fs.Cust_id group by dc.Cust_id, dc.product_category;
# SELECT cust_id from demo where sum_sales > 5000
