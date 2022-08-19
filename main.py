import material 
import json
import virtual
import pandasql
import xml.etree.ElementTree as ET


virtual_views_dict={}

with open("./virtual_views.json", 'r') as j:
    virtual_views_dict = json.loads(j.read())

with open("./view_types.json", 'r') as j:
    view_dict = json.loads(j.read()) 

def create_view(sql):
    sql_lst=sql.lower().split()
    viewname = sql.split()[2]

    if(viewname in list(view_dict.keys())):
        return "viewname already exists"
        
    
    if(sql_lst[-1]=='virtual'):
        view_dict[viewname]='virtual'
        sql=sql[:-8]
        virtual_views_dict[viewname]=sql
        print(virtual_views_dict[viewname])

        with open("virtual_views.json", "w") as outfile:
            json.dump(virtual_views_dict, outfile)
        with open("view_types.json", "w") as outfile:
            json.dump(view_dict, outfile)

        return "virtual view saved"
    elif(sql_lst[-1]=='material'):
        #CREATE VIEW
        view_dict[viewname]='material'
        sql_not_mat=sql_lst[:-1]
        sql=sql[:-9]
        virtual_views_dict[viewname]=sql
        print(virtual_views_dict)

        with open("virtual_views.json", "w") as outfile:
            json.dump(virtual_views_dict, outfile)
        with open("view_types.json", "w") as outfile:
            json.dump(view_dict, outfile)
        
        print(sql)
        columns = material.parsing(sql=sql)
        material.generateDataFrames(columns, material.rdbms, material.csvinfo, sql)
        req_view = material.getView(viewname)
        return req_view
def run_query_material(sql,viewname):
    print(sql)
    df =  material.QueryView(sql)
    return df

def run_query_virtual(sql,viewname):
    # with open("/Users/jaideepkukkadapu/Documents/spe/dm_proj/virtual_views.json", 'r') as j:
        # virtual_views_dict = json.loads(j.read())
    view_definition_query = virtual_views_dict[viewname]   

    reqdf, df_list = virtual.createView(view_definition_query)
    output = virtual.runQuery(reqdf, df_list, sql, viewname)
    return output

def run_query(sql):
    sql_lst=sql.lower().split()
    i=sql_lst.index('from')
    viewname=sql_lst[i+1]
    #print("___________view",viewname)
    if(view_dict[viewname]=='virtual'):
        return run_query_virtual(sql,viewname)
    elif(view_dict[viewname]=='material'):
        sql = sql.replace(viewname, viewname+"_view")
        return run_query_material(sql,viewname)


def getViewNameList():
    view_lst = list(view_dict.keys())
    return view_lst

def getViewType(viewname):
    return view_dict[viewname]

def refreshView(viewname, sql):

    material.dropView(viewname)
    req_view_def = virtual_views_dict[viewname]
    print(req_view_def)
    columns = material.parsing(sql=req_view_def)
    material.generateDataFrames(columns, material.rdbms, material.csvinfo, req_view_def)
    sql = sql.replace(viewname, viewname+"_view")
    print(sql)	
    df =  material.QueryView(sql)
    return df
        #req_view = material.getView(viewname)


def addDataSource(dataSourceType, dataSourceInfo):
    
    if(dataSourceType == "rdbms"):
        root = material.tree.getroot()
        newSource = ET.Element('rdbms_datasource')
        sourceLocation = ET.SubElement(newSource, 'location')
        sourceLocation.text = dataSourceInfo['location']
        sourceuser = ET.SubElement(newSource, 'user-name')
        sourceuser.text = dataSourceInfo['user-name']
        sourcepass = ET.SubElement(newSource, 'password')
        sourcepass.text = dataSourceInfo['password']
        sourceDetails = ET.SubElement(newSource, 'database_details')
        sourcedbname = ET.SubElement(sourceDetails, 'dbname') 
        sourcedbname.text = dataSourceInfo['dbname']
        sourcetable = ET.SubElement(sourceDetails, 'table_name')
        sourcetable.text = dataSourceInfo['table_name']
        
        root.append(newSource)
    
    if(dataSourceType == "csv"):
        root = material.tree.getroot()
        newSource = ET.Element('csv_datasource')
        sourceDetails=ET.SubElement(newSource,'database_details')
        sourcename = ET.SubElement(sourceDetails, 'csvname')
        sourcename.text = dataSourceInfo['csvname']
        sourceloc = ET.SubElement(sourceDetails, 'csv_loc')
        sourceloc.text = dataSourceInfo['csv_loc']
        
        root.append(newSource)		
    
    new_xml_tree_string = ET.tostring(root)
    with open("./connecting.xml", "wb") as fn:
        print("writing to file")
        fn.write(new_xml_tree_string)
        

#------------------------------TESTBENCH FOR CHECKING COMPLETE BACKEND----------------------------------------
# def main():
#     sql1="CREATE VIEW testing1 as select dc.Cust_id, dc.product_category, sum(fs.Sales) as sum_sales from csv$star as fs inner join ( select pq.product_category, ls.cust_id from sql$marketdb$dim_prod as pq inner join sql$marketdb$fact_sales as ls on pq.prod_id == ls.prod_id ) as dc on dc.Cust_id == fs.Cust_id group by dc.Cust_id, dc.product_category material"
#     #sql2="CREATE VIEW demo2 as select dc.Cust_id, dc.product_category, sum(fs.Sales) as sum_sales from csv$star as fs inner join ( select pq.product_category, ls.cust_id from sql$marketdb$dim_prod as pq inner join sql$marketdb$fact_sales as ls on pq.prod_id == ls.prod_id ) as dc on dc.Cust_id == fs.Cust_id group by dc.Cust_id, dc.product_category virtual"
#     #sql3="CREATE VIEW demo3 as select dc.Cust_id, dc.product_category, sum(fs.Sales) as sum_sales from csv$star as fs inner join ( select pq.product_category, ls.cust_id from sql$marketdb$dim_prod as pq inner join sql$marketdb$fact_sales as ls on pq.prod_id == ls.prod_id ) as dc on dc.Cust_id == fs.Cust_id group by dc.Cust_id, dc.product_category material"
#     #sql4="CREATE VIEW demo5 as select dc.Cust_id, dc.product_category, sum(fs.Sales) as sum_sales from csv$star as fs inner join ( select pq.product_category, ls.cust_id from sql$marketdb$dim_prod as pq inner join sql$marketdb$fact_sales as ls on pq.prod_id == ls.prod_id ) as dc on dc.Cust_id == fs.Cust_id group by dc.Cust_id, dc.product_category material"
#     query="SELECT cust_id from testing1 where sum_sales > 5000 limit 5"
    
#     create_view(sql1)
#     #create_view(sql2)
#     #create_view(sql3)
#     #create_view(sql4)
#     df = run_query(query)
#     print(df)
    
#     # t1 = "rdbms"
#     # t2 = "csv"
    
#     # d1 = {'location' : 'localhost', 'user-name' : 'root', 'password' : 'lana2I3BQC?', 'dbname' : 'marketdb', 'table_name' : 'fact_sales'}
    
#     # d2 = {'csvname' : 'employee', 'csv_loc' : '/home/suchit/Desktop/dm_proj/employee.csv'}
    
#     # addDataSource(t1, d1)
#     # addDataSource(t2, d2)

#main()
    
