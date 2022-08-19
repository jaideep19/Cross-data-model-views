from tkinter import *
from tkinter import ttk
import pandas as pd

from datetime import datetime
from pandastable import Table
import main

class App:
	
	def __init__(self):
		self.mainWindow = Tk()
		self.mainWindow.title('Cross Data Model Views')
		#self.TableWindow.title("Table Display")
		self.mainWindow.geometry("600x250")
		
		self.view_definition_query = ""
		self.running_query = ""
		self.viewname = ""
		
		self.startUp()
	
	def startUp(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
		
		self.startFrame = Frame(self.mainWindow, relief=RAISED, borderwidth=1)
		#self.startFrame.pack(fill=BOTH, expand=True)
		self.frameTitle = Label(self.mainWindow, text="CROSS DATA MODEL VIEWS", font=("Arial", 25), wraplength=500)
		self.frameTitle.pack(side=TOP, padx=5, pady=5)
		self.createButton = ttk.Button(self.mainWindow, text = "Create View", command = self.createView)
		#self.createButton.grid(row = ,column = 0)
		self.createButton.pack(side=TOP, pady=15, padx=15)
		
		self.queryButton = ttk.Button(self.mainWindow, text = "Query Existing View", command = self.queryView)
		#self.queryButton.grid(row = 1, column = 0)
		#self.createButton.pack()
		self.queryButton.pack(side=TOP, padx = 15, pady=10)
		self.addsourceButton = ttk.Button(self.mainWindow, text = "Add New Data Source", command = self.addSource)
		self.addsourceButton.pack(side = TOP, padx = 15, pady = 10)
	
	
	
	def addSource(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
		self.addsourceType = ""
		self.addsourceTypeVar = StringVar()
		self.addsourceInfo = {}
		
		self.addSourceFrame1 = Frame(self.mainWindow)
		
		
		self.sourceType = {"Relational Data Model" : "rdbms", "CSV Datal Model" : "csv"}
		self.selectedType = StringVar()
		self.addsourceTypelabel = Label(self.mainWindow, text = "Select The Type of Data Source to be Added")
		self.addsourceTypelabel.pack(side="top", padx = 10, pady = 10)
		self.addsourceRadioFrame = Frame(self.mainWindow)
		for (text, value) in self.sourceType.items():
			Radiobutton(self.addsourceRadioFrame, text = text, variable = self.addsourceTypeVar, value = value).pack(side="right", padx = 10, pady = 10)
		
		self.addsourceRadioFrame.pack(side = "top", padx = 10, pady = 10)
		
		self.nextstepButton = ttk.Button(self.mainWindow, text = "Add Details", command = self.addSourceDetails)
		self.nextstepButton.pack(side=BOTTOM, padx = 10, pady = 10)
	
	def addSourceDetails(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
		self.addsourceType = self.addsourceTypeVar.get()
		if(self.addsourceType == "rdbms"):
			self.addSourceFrame1 = Frame(self.mainWindow)
			
			self.addLocation = Label(self.mainWindow, text = "Enter Database Location")
			self.addLocation.pack(side = "top", padx = 10,pady=10)
			self.addLocationEntry = Entry(self.mainWindow, width = 80)
			self.addLocationEntry.pack(side="top", padx = 10,pady=10)
			
			self.addDetailsFrame1 = Frame(self.mainWindow)
			self.adduser = Label(self.addDetailsFrame1, text = "Enter Username")
			self.adduser.pack(side = "left", padx = 5,pady=10)
			self.adduserEntry = Entry(self.addDetailsFrame1, width = 20)
			self.adduserEntry.pack(side="left", padx = 5,pady=10)
			self.addpass = Label(self.addDetailsFrame1, text = "Enter Password")
			self.addpass.pack(side = "left", padx = 5,pady=10)
			self.addpassEntry = Entry(self.addDetailsFrame1, width = 20)
			self.addpassEntry.pack(side="left", padx = 5,pady=10)
			
			self.addDetailsFrame1.pack(side=TOP, padx = 10, pady = 10)
			
			self.addDetailsFrame2 = Frame(self.mainWindow)
			self.adddbname = Label(self.addDetailsFrame2, text = "Enter Database Name")
			self.adddbname.pack(side = "left", padx = 5,pady=10)
			self.adddbnameEntry = Entry(self.addDetailsFrame2, width = 20)
			self.adddbnameEntry.pack(side="left", padx = 5,pady=10)
			self.addtable = Label(self.addDetailsFrame2, text = "Enter Table Name")
			self.addtable.pack(side = "left", padx = 5,pady=10)
			self.addtableEntry = Entry(self.addDetailsFrame2, width = 20)
			self.addtableEntry.pack(side="left", padx = 5,pady=10)
			
			self.addDetailsFrame2.pack(side=TOP, padx = 10, pady = 10)
			
			 
			
			self.nextstepButton1 = ttk.Button(self.mainWindow, text = "Add datasource", command = self.addSourcetoXML)
			self.nextstepButton1.pack(side=BOTTOM, padx = 10, pady = 10)
			
		else:
		
			self.addSourceFrame1 = Frame(self.mainWindow)
			
			self.addcsvname = Label(self.mainWindow, text = "Enter CSV File Name")
			self.addcsvname.pack(side = "top", padx = 10,pady=10)
			self.addcsvnameEntry = Entry(self.mainWindow, width = 80)
			self.addcsvnameEntry.pack(side="top", padx = 10,pady=10)
			
			self.addcsvloc = Label(self.mainWindow, text = "Enter CSV File Location")
			self.addcsvloc.pack(side = "top", padx = 10,pady=10)
			self.addcsvlocEntry = Entry(self.mainWindow, width = 80)
			self.addcsvlocEntry.pack(side="top", padx = 10,pady=10)
			
			
			self.nextstepButton2 = ttk.Button(self.mainWindow, text = "Add datasource", command = self.addSourcetoXML)
			self.nextstepButton2.pack(side=BOTTOM, padx = 10, pady = 10)
			
			
			
			
	def addSourcetoXML(self):
		
		if(self.addsourceType == "rdbms"):
			self.addsourceInfo['location'] = self.addLocationEntry.get()
			self.addsourceInfo['user-name'] = self.adduserEntry.get()
			self.addsourceInfo['password'] = self.addpassEntry.get()
			self.addsourceInfo['dbname'] = self.adddbnameEntry.get()
			self.addsourceInfo['table_name'] = self.addtableEntry.get()
		else:
			self.addsourceInfo['csvname'] = self.addcsvnameEntry.get()
			self.addsourceInfo['csv_loc'] = self.addcsvlocEntry.get()

		for i in self.mainWindow.winfo_children():
				i.destroy()
		main.addDataSource(self.addsourceType,self.addsourceInfo)
		self.addFinalFrame = Frame(self.mainWindow)
		self.addsuccessLabel = Label(self.mainWindow, text="DATASOURCE IS ADDED! MOVE TO HOME PAGE BY CLICKING BELOW", font=("Arial", 25), wraplength=500)
		self.addsuccessLabel.pack(side=TOP, padx=5, pady=5)
		self.addFinalFrame.pack(side=TOP, padx=5, pady=5)
		self.finalButton = ttk.Button(self.mainWindow, text = "Go to Home", command = self.startUp)
		self.finalButton.pack(side=BOTTOM, padx = 10, pady = 10)
		
			
	
	def createView(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
		
		self.createViewFrame = Frame(self.mainWindow)
		
		self.viewType = {"Virtual" : "virtual", "Material" : "material"}
		self.selectedType = StringVar()
		self.viewdeftype = Label(self.mainWindow, text = "Select The View Type to be Created")
		self.viewdeftype.pack(side="top", padx = 10, pady = 10)
		self.radioFrame = Frame(self.mainWindow)
		for (text, value) in self.viewType.items():
			Radiobutton(self.radioFrame, text = text, variable = self.selectedType, value = value).pack(side="right", padx = 10, pady = 10)
		self.radioFrame.pack(side="top", padx = 10, pady = 10)
		self.viewDefinition = Label(self.mainWindow, text = "Enter View Definition")
		self.viewDefinition.pack(side = "top", padx = 10,pady=10)
		self.viewEntry = Entry(self.mainWindow, width = 80)
		self.viewEntry.pack(side="top", padx = 10,pady=10)
		
		self.createButton = ttk.Button(self.mainWindow, text = "Create View", command = self.processInput)
		self.createButton.pack(side=BOTTOM, padx=15, pady=5)
		
	def processInput(self):
	
		self.view_definition_query = self.viewEntry.get()
		self.view_definition_query = self.view_definition_query.rstrip()
		print(self.selectedType.get())
		
		self.view_definition_query = self.view_definition_query + " " + self.selectedType.get()
		print(self.view_definition_query)
		
		response = main.create_view(self.view_definition_query)
		
		if(not isinstance(response, pd.DataFrame)):

			if(response == "viewname already exists"):
				print("error handle")
				self.viewnameAlreadyThere()
			else:
				print("reached here")
				self.successCreation()
		
		else:
			print(response)
			self.TableWindow = Tk()
			self.TableWindow.title("Table Window")
			
			self.TableWindow.geometry("600x500")
			self.tableframeTitle = Label(self.TableWindow, text="GENERATED MATERIAL VIEW.", font=("Arial", 25), wraplength=500)
			self.tableframeTitle.pack(side=TOP, padx=5, pady=5)
			tableFrame = Frame(self.TableWindow)
			tableFrame.pack(fill='both', expand=True)
			table = Table(tableFrame, dataframe = response, showtoolbar = False, showstatusbar = False)
			table.show()
			
			self.moveToQuerybutton = ttk.Button(self.TableWindow, text = "Move to Query Window", command = self.moveToQuery)
			self.moveToQuerybutton.pack(side=BOTTOM)
			
		'''columns = tools.parsing(self.view_definition_query)
		tools.generateDataFrames(columns, tools.rdbms, tools.csvinfo, self.view_definition_query)
		df = tools.getView(self.view_definition_query.split()[2])
		print(df)
		

		
		self.TableWindow = Tk()
		self.TableWindow.title("Table Window")
		self.TableWindow.geometry("500x500")
		tableFrame = Frame(self.TableWindow)
		tableFrame.pack(fill='both', expand=True)
		table = Table(tableFrame, dataframe = df, showtoolbar = False, showstatusbar = False)
		table.show()
		
		self.moveToQuerybutton = ttk.Button(self.TableWindow, text = "Move to Query Window", command = self.moveToQuery)
		self.moveToQuerybutton.pack(side=BOTTOM)'''
	def successCreation(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
		self.successFrame = Frame(self.mainWindow, relief=RAISED, borderwidth=1)
		
		self.successframeTitle = Label(self.mainWindow, text="VIRTUAL VIEW HAS BEEN CREATED SUCCESSFULLY.", font=("Arial", 25), wraplength=500)
		self.successframeTitle.pack(side=TOP, padx=5, pady=5)
		
		self.successmoveButton = ttk.Button(self.mainWindow, text = "Move to Query Window", command = self.queryView)
		
		self.successmoveButton.pack(side=BOTTOM, pady=15, padx=15)
	
	def viewnameAlreadyThere(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
		self.redoFrame = Frame(self.mainWindow, relief=RAISED, borderwidth=1)
		
		self.redoframeTitle = Label(self.mainWindow, text="A VIEW WITH THE GIVEN VIEWNAME ALREADY EXISTS. PLEASE ENTER A DIFFERENT VIEW NAME", font=("Arial", 25), wraplength=500)
		self.redoframeTitle.pack(side=TOP, padx=5, pady=5)
		
		self.redomoveButton = ttk.Button(self.mainWindow, text = "Move to Create Window", command = self.createView)
		
		self.redomoveButton.pack(side=BOTTOM, pady=15, padx=15)

		
	def moveToQuery(self):
		self.TableWindow.destroy()
		self.queryView()

	
	def queryView(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
			
		self.createQueryFrame = Frame(self.mainWindow)
		self.viewlist = main.getViewNameList()
		print("printing view list", self.viewlist)
		#self.selectedView = StringVar()
		#elf.selectviewFrame = Frame(self.mainWindow)
		#self.selectviewLabel = Label(self.selectviewFrame, text = "Select your View to Query")
		#self.selectviewLabel.pack(side="left", padx = 10, pady = 10)
		#self.selectOption = OptionMenu(self.selectviewFrame, self.selectedView, *self.viewlist).pack(side = "right", padx = 10, pady = 10)
		
		#self.selectviewFrame.pack(side = TOP, padx = 10, pady = 10)
		self.queryLabel = Label(self.mainWindow, text = "Enter Query")
		self.queryLabel.pack(side="top", padx = 5,pady=5)
		self.queryEntry = Entry(self.mainWindow, width = 80)
		self.queryEntry.pack(side="top", padx = 5,pady=5)
		
		self.processButton = ttk.Button(self.mainWindow, text = "Process Query", command = self.processQuery)
		self.processButton.pack(side="bottom")
		self.home1Button = ttk.Button(self.mainWindow, text = "Move to Home", command = self.startUp)
		self.home1Button.pack(side="bottom")
		
	def processQuery(self):
		
		self.running_query = self.queryEntry.get()
		self.running_query_list = self.running_query.split()
		for i,val in enumerate(self.running_query_list):
			if(val.lower() == "from"):
				self.viewname = self.running_query_list[i+1]
		print(self.viewname) 
		self.selectedViewType = main.getViewType(self.viewname)
		
		print(self.running_query)
		if(self.selectedViewType == "virtual"):
		
			response = main.run_query(self.running_query)
			print(response)
			self.TableWindow = Tk()
			self.TableWindow.geometry("600x520")
			self.tableframeTitle = Label(self.TableWindow, text="QUERIED VIRTUAL VIEW.", font=("Arial", 25), wraplength=500)
			self.tableframeTitle.pack(side=TOP, padx=5, pady=5)
			self.TableWindow.title("Table Window")
			tableFrame = Frame(self.TableWindow, width=500, height=500)
			tableFrame.pack(fill='both', expand=True)
			table = Table(tableFrame, dataframe = response, showtoolbar = False, showstatusbar = False)
			table.show()
			
			self.homeButton = ttk.Button(self.TableWindow, text = "Close Window", command = self.moveToQuery)
			self.homeButton.pack(side=BOTTOM)
		
		else:
			self.materialresponse = main.run_query(self.running_query)
			print(self.materialresponse)
			self.TableWindow = Tk()
			self.TableWindow.geometry("600x550")
			self.tableframeTitle = Label(self.TableWindow, text="QUERIED MATERIAL VIEW.", font=("Arial", 25), wraplength=500)
			self.tableframeTitle.pack(side=TOP, padx=5, pady=5)
			self.TableWindow.title("Table Window")
			self.materialtableFrame = Frame(self.TableWindow, width=500, height=500)
			self.materialtableFrame.pack(fill='both', expand=True)
			self.materialtable = Table(self.materialtableFrame, dataframe = self.materialresponse, showtoolbar = False, showstatusbar = False)
			self.materialtable.show()
			self.buttonsFrame = Frame(self.TableWindow)
			self.homeButton = ttk.Button(self.buttonsFrame, text = "Close Window", command = self.moveToQuery)
			self.homeButton.pack(side=LEFT)
			self.refreshButton = ttk.Button(self.buttonsFrame, text = "Refresh View", command = self.refreshView)
			self.refreshButton.pack(side=RIGHT)
			self.buttonsFrame.pack(side=BOTTOM, padx = 10, pady = 10)
			

	def refreshView(self):
		#self.running_query = self.running_query+"5"
		self.materialresponse = main.refreshView(self.viewname, self.running_query)
		print(self.materialresponse)
		for i in self.TableWindow.winfo_children():
			i.destroy()
		#self.materialtable = Table(self.materialtableFrame, dataframe = refreshed_response, showtoolbar = False, showstatusbar = False)
		self.tableframeTitle = Label(self.TableWindow, text="QUERIED MATERIAL VIEW.", font=("Arial", 25), wraplength=500)
		self.tableframeTitle.pack(side=TOP, padx=5, pady=5)
		self.TableWindow.title("Table Window")
		self.materialtableFrame = Frame(self.TableWindow, width=500, height=500)
		self.materialtableFrame.pack(fill='both', expand=True)
		self.materialtable = Table(self.materialtableFrame, dataframe = self.materialresponse, showtoolbar = False, showstatusbar = False)
		self.materialtable.show()
		self.buttonsFrame = Frame(self.TableWindow)
		self.homeButton = ttk.Button(self.buttonsFrame, text = "Kill View", command = self.moveToQuery)
		self.homeButton.pack(side=LEFT)
		self.refreshButton = ttk.Button(self.buttonsFrame, text = "Refresh View", command = self.refreshView)
		self.refreshButton.pack(side=RIGHT)
		self.buttonsFrame.pack(side=BOTTOM, padx = 10, pady = 10)
		
		
		
	def run(self):
		self.mainWindow.mainloop()
		

app = App()
app.run()

		
		
		
		



'''from tkinter import *
from tkinter import ttk
import pandas as pd

from datetime import datetime
from pandastable import Table
from simplegui import LARGEFONT
import tools

class App:
	
	def __init__(self):
		self.mainWindow = Tk()
		self.mainWindow.title('Cross Data Model Views')
		#self.TableWindow.title("Table Display")
		self.mainWindow.geometry("800x800")
		
		self.view_definition_query = ""
		self.running_query = ""
		self.viewname = ""
		
		self.startUp()
	
	def startUp(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
		
		self.startFrame = Frame(self.mainWindow, relief=RAISED, borderwidth=1)
		#self.startFrame.pack(fill=BOTH, expand=True)
		self.frameTitle = Label(self.mainWindow, text="CROSS DATA MODEL VIEWS", font = LARGEFONT, wraplength=300)
		self.frameTitle.pack(side=TOP, padx=5, pady=5)
		self.createButton = ttk.Button(self.mainWindow, text = "Create View", command = self.createView)
		#self.createButton.grid(row = ,column = 0)
		self.createButton.pack(side=TOP, pady=15, padx=15)
		
		self.queryButton = ttk.Button(self.mainWindow, text = "Query Existing View", command = self.queryView)
		#self.queryButton.grid(row = 1, column = 0)
		#self.createButton.pack()
		self.queryButton.pack(side=TOP, padx = 15, pady=10)
		
	
	def createView(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
		
		self.createViewFrame = Frame(self.mainWindow)
		self.viewDefinition = Label(self.mainWindow, text = "Enter View Definition")
		self.viewDefinition.grid(row = 0, column = 0)
		self.viewEntry = Entry(self.mainWindow, width = 50)
		self.viewEntry.grid(row = 0, column = 1, columnspan = 2, pady = 5, padx = 5)
		
		self.createButton = ttk.Button(self.mainWindow, text = "Create View using Definition", command = self.processInput)
		self.createButton.grid(row = 1, column = 1)
		
	def processInput(self):
		self.view_definition_query = self.viewEntry.get()
		print(self.view_definition_query)
		columns = tools.parsing(self.view_definition_query)
		tools.generateDataFrames(columns, tools.rdbms, tools.csvinfo, self.view_definition_query)
		df = tools.getView(self.view_definition_query.split()[2])
		print(df)
		
		TableWindow = Tk()
		TableWindow.title("Table Window")
		tableFrame = Frame()
		table = Table(TableWindow, dataframe = df, showtoolbar = True, showstatusbar = True)
		table.show()
		
		self.moveToQuerybutton = ttk.Button(TableWindow, text = "Move to Query Window", command = self.queryView)

	
	def queryView(self):
		for i in self.mainWindow.winfo_children():
			i.destroy()
			
		self.createQueryFrame = Frame(self.mainWindow)
		self.viewNameLabel = Label(self.mainWindow, text = "Enter View Name")
		
		self.viewNameLabel.grid(row = 0, column = 0)
		self.viewNameEntry = Entry(self.mainWindow, width = 30)
		self.viewNameEntry.grid(row = 0, column = 2, columnspan = 2, pady = 15, padx = 5)
		
		self.queryLabel = Label(self.mainWindow, text = "Enter Query")
		self.queryLabel.grid(row = 1, column = 0)
		self.queryEntry = Entry(self.mainWindow, width = 50)
		self.queryEntry.grid(row = 1 ,column = 2, columnspan = 2, pady = 20,padx = 5)
		
		self.processButton = ttk.Button(self.mainWindow, text = "Process Query", command = self.processQuery)
		self.processButton.grid(row = 2, column = 1, padx=5, pady=25)
		self.home1Button = ttk.Button(self.mainWindow, text = "Move to Home", command = self.startUp)
		self.home1Button.grid(row = 2, column =3 )

	def queryView(self):
		self.f1 = Frame(self.mainWindow, borderwidth=5, relief="ridge", width=500, height=500)
		self.viewNameLabel = Label(self.mainWindow, text = "Enter View Name")
		self.viewNameEntry = Entry(self.mainWindow, width = 30)
		self.queryLabel = Label(self.mainWindow, text = "Enter Query")
		self.queryEntry = Entry(self.mainWindow, width = 50)
		self.processButton = ttk.Button(self.mainWindow, text = "Process Query", command = self.processQuery)
		self.home1Button = ttk.Button(self.mainWindow, text = "Move to Home", command = self.startUp)
		
		#self.mainWindow.grid(column=0, row=0)
		self.f1.grid(column=0, row=0, columnspan=3, rowspan=2)
		self.viewNameLabel.grid(column=3, row=0, columnspan=2)
		self.viewNameEntry.grid(column=4, row=0, columnspan=2)
		self.queryLabel.grid(column=3, row=1, columnspan=2)
		self.queryEntry.grid(column=4, row =1, columnspan=2)
		self.processButton.grid(column=3, row=3)
		self.home1Button.grid(column=4, row=3)

		
	def processQuery(self):
	
		self.viewname = self.viewNameEntry.get()
		print(self.viewname)
		self.running_query = ""
		self.running_query = self.queryEntry.get()
		if(len(self.running_query) == 0):
			self.running_query = "select * from {}".format(self.viewname)
		
		self.running_query = self.running_query.replace(self.viewname, self.viewname+"_view")
		
		df = tools.QueryView(self.running_query)
		print(df)
		df = pd.DataFrame({'num_legs': [2, 4, 8, 0],
                   'num_wings': [2, 0, 0, 0],
                   'num_specimen_seen': [10, 2, 1, 8]},
                  index=['falcon', 'dog', 'spider', 'fish'])
		TableWindow = Tk()
		TableWindow.geometry("500x500")
		TableWindow.title("Table Window")
		self.tableFrame = Frame(TableWindow)
		self.tableFrame.pack()
		table = Table(TableWindow, dataframe = df, showtoolbar = False, showstatusbar = False)
		#table.grid(row=3)
		table.show()
		
		#self.homeButton = ttk.Button(TableWindow, text = "Move to Query Window", command = self.StartUp)
		
	def run(self):
		self.mainWindow.mainloop()
		

app = App()
app.run()'''

'''self.viewname = 
		print(self.viewname)
		self.running_query = ""
		self.running_query = self.queryEntry.get()
		if(len(self.running_query) == 0):
			self.running_query = "select * from {}".format(self.viewname)
		
		self.running_query = self.running_query.replace(self.viewname, self.viewname+"_view")
		
		df = tools.QueryView(self.running_query)
		print(df)
		self.TableWindow = Tk()
		self.TableWindow.geometry("500x500")
		self.TableWindow.title("Table Window")
		tableFrame = Frame(self.TableWindow, width=500, height=500)
		tableFrame.pack(fill='both', expand=True)
		table = Table(tableFrame, dataframe = df, showtoolbar = False, showstatusbar = False)
		table.show()
		
		self.homeButton = ttk.Button(self.TableWindow, text = "Kill View", command = self.moveToQuery)
		self.homeButton.pack(side=BOTTOM)'''

		
		
		
		
