'''
  This module handles the data basae. It takes in commnads and then translates them into sql
  then passes that onto the data base. 
'''
import sqlite3
import time
import pandas as pd
import matlab.engine # this is so the data base can see the matlab data types

from database_python_api.dataTypesImporter import dataTypeImporter # pylint: disable=e0401
from database_python_api.dataType import dataType # pylint: disable=e0401
from logging_system_display_python_api.logger import loggerCustom # pylint: disable=e0401
from threading_python_api.threadWrapper import threadWrapper # pylint: disable=e0401


class DataBaseHandler(threadWrapper):
    '''
        calling the init function will create the basics for the class and then 
        add the create_data_base to the queue for task to be done, when you start 
        the run function. To be clear this class should work just fine in a single 
        thread, just dont call run, makeRequest and getRequest. However, when 
        running multi thread, the user must call run FIRST, then they can use the
        make request and get request to interface with the class. 
        NOTE: When running multi threaded, only the __init__, makeRequest, getRequest, 
        and run function should be called by out side classes and threads. 
    '''   
    def __init__(self, coms, db_name = 'database/database_file'):
        #make class matiance vars
        self.__logger = loggerCustom("logs/database_log_file.txt")
        self.__coms = coms
        self.__db_name = db_name
        self.__conn = None
        self.__c = None
        self.__tables = None

        #make Maps for db creation
        self.__type_map = { #the point of this dictinary is to map the type names from the
                            # dataTypes.dtobj file to the sql data base.
            "int" : "INTEGER", 
            "float" : "FLOAT(10)", # NOTE: the (#) is the perscition of the float. 
            "string" : "TEXT",
            "bool" : "BOOLEAN",
            "bigint" : "BIGINT"
        } #  NOTE: this dict makes the .dtobj file syntax match sqlite3 syntax.

        #Start the threaed wrapper for  the process
        super().__init__()

        #send request to parent class to make the data base
        super().make_request('create_data_base', [])
    def create_data_base(self):
        '''
        Makes the data base.
        NOTE: if you want to change the name of the data base then that needs 
        to be done when you make the class
        '''
        #Make data base (bd)
        self.__conn = sqlite3.connect(self.__db_name)
        self.__c = self.__conn.cursor()

        #find and create the data tables fro the data base
        tableFinder = dataTypeImporter(self.__coms)
        tableFinder.pasre_data_types()
        self.__tables = tableFinder.get_data_types() #this varible will be used for creatig/accessing/parsing the data. 
        #  In other words its super imporatant.  

        #create a table in the data bas for each data type
        for table in self.__tables:
            self.create_table([table])
        self.__logger.send_log("Created database:\n" + self.get_tables_html())
        self.__coms.print_message("Created database", 2)   
    def create_table(self, args):
        '''
            This function looks to create a table. If the table already exsists if will not create it. 

            NOTE: This function is NOT for other threads to call! Use the create_table_external func.
            args[0] : is the table name
        '''
        table_name = self.__tables[args[0]].get_data_group()
        table_feilds = self.__tables[args[0]].get_fields()
        db_command = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        #this line is add as a way to index the data base
        db_command += "[table_idx] INT PRIMARY KEY" 

        #iter for every feild in this data group. The feilds come from the dataTypes.dtobj file. 
        for feild_name in table_feilds:
            # NOTE: in the dataTypes class the feilds dict maps to a tuple of (bits, convert_type)
            # the bits is not used here because it is for collecting the data. The convert typ is,
            # thus we want to access feild 1 of the tuple. In addition that is fed into the 
            # self.__type_map to convert it to SQL syntax
            #we dont need feilds for igrnoed data feilds
            if feild_name!= "igrnoed feild" and table_feilds[feild_name][1] != "NONE":
                # adding the ", " here means we don't have an extra one on the last line.
                db_command += ", "
                db_command += f"[{feild_name}] {self.__type_map[table_feilds[feild_name][1]]}" 
        db_command += ")"

        #try to make the table in the data base
        try :  
            self.__c.execute(db_command)
            self.__logger.send_log("Created table: " + db_command)
            self.__coms.print_message("Created table: " + db_command, 2)
        except Exception as error: # pylint: disable=w0718
            self.__coms.print_message(str(error) + " Command send to db: " + db_command, 0) 
            self.__logger.send_log("Failed to created table: " + db_command + str(error))
            self.__coms.print_message("Failed to created table: " + db_command + str(error), 0)
    def insert_data(self, args, idx_in = -1):
        '''
            This func takes in the table_name to insert and a list of data, 
            the list must be in the same order that is defined in the .dtobj file.
            args is a list were the fist index is the table name and 
            the second is the data
            Args:
                [0] : table name
                [1] : list of data
        '''
        #first step get the last saved idx
        if idx_in == -1:
            self.__c.execute(f"SELECT * FROM {args[0]} WHERE table_idx = (SELECT max(table_idx) FROM {args[0]})")
            data = pd.DataFrame(self.__c.fetchall()) 
            if data.empty:
                idx = 0
            else :
                idx  = data[0][0] + 1
        else :
            idx = idx_in # if we pass the index in we save time
        # idx = 0 #if the above command fails it means there is no data saved into the data base and we need to start the index at 0.
        db_command = f"INSERT INTO {args[0]} (table_idx"
        #get the data type obj and then get the feilds list
        feilds = self.get_feilds([self.get_data_type([args[0]])]) 
        for feild_name in feilds:
            #we dont need feilds for igrnoed data feilds
            if feild_name!= "igrnoed feild" and feilds[feild_name][1] != "NONE" : 
                db_command += ", "
                db_command += f"{feild_name}"
        db_command +=  ") "
        db_command += f"VALUES ({idx}"

        for val in args[1]: #args[1] is the data
            db_command += ", "
            # for sql str has a special format so we need the if statment

            if not isinstance(val, str):
                db_command += f"{val}"
            else :
                db_command += f"'{val}'" #Notice that there is '' on this line.           
        db_command += ");"
        try:
            self.__c.execute(db_command)
        except Exception as error:
            self.__coms.print_message(str(error) + " Command send to db: " + db_command, 0) 
            self.__logger.send_log(str(error) + " Command send to db: " + db_command)
            # pylint: disable=w0707
            # pylint: disable=w0719
            raise Exception
        return "Complete"
    #some  useful getters
    def get_tables_html(self):
        # pylint: disable=missing-function-docstring
        message = "<! DOCTYPE html>\n<html>\n<body>\n<h1>DataBase Tables</h1>"
        for table in self.__tables: 
            message +=f"<p><strong>Table:</strong> {table}</p>\n" # pylint: disable=r1713 
        message += "</body>\n</html>"
        return message
    def get_tables_str_list(self):
        # pylint: disable=missing-function-docstring
        strList = []
        for table in self.__tables:
            strList.append(table)
        return strList
    def get_data_type(self, args):
        '''
            args is a list where the fist index is the table name
        '''
        return self.__tables[args[0]] # the arg is the table name to find
    def get_feilds(self, args):
        '''
            args is a list where the fist index is a data type obj 
        '''
        return args[0].get_fields() # the args is a data type obj in the first index of the list
    def get_feilds_list(self, args):
        '''
            args is a list where the fist index is a data type obj 
        '''
        feilds = args[0].get_fields()# the args is a data type obj in the first index of the list
        feilds_list = []
        for feild in feilds:
            if feilds[feild][1] != "NONE": #dont add any feilds that are not in the data base
                feilds_list.append(feild)
        return feilds_list
    def get_data(self, args):
        '''
            args is a list where the fist index is the table name 
            and the second is the start time for collecting the data
            ARGS:
                [0] table name
                [1] table_idx (starting indx)
            RETURNS:
                html string with data
            NOTE: This function is NOT meant for larg amouts of data! Make request small request to this function of it will take a long time. 
        '''
        message = f"<h1>{args[0]}: " 
        try :
            #from and run db command
            db_command = f"SELECT * FROM {args[0]} WHERE table_idx >= {str(args[1])} ORDER BY table_idx"
            self.__c.execute(db_command)
            self.__logger.send_log("Query command recived: "  + db_command)
            self.__coms.print_message("Query command recived: "  + db_command, 2)
        except Exception as error: # pylint: disable=w0718
            self.__coms.print_message(str(error) + " Command send to db: " + db_command, 0)
            self.__logger.send_log(str(error) + " Command send to db: " + db_command)
            return "<p> Error getting data </p>"
        #get cols 
        cols = ["Table Index "] # add table_idx to the cols lis
        cols += self.get_feilds_list([self.get_data_type([args[0]])])
        message += f"{cols}</h1> "
        #fetch and convert the data into a pandas data frame.
        data = pd.DataFrame(self.__c.fetchall(), columns=cols)      
        for idx in range(len(data)):
            message += "<p>"
            for i in range(len(cols) - 1): #itrate for all but last col
                message += f"{data.iloc[idx,i]},"
                message += f"{data.iloc[idx,len(cols) - 1]}"# add last col with out ,
            message += "</p>"
        self.__logger.send_log("data collected: " + message)
        return message
        #this is the setter section
    def create_table_external(self, args):
        '''
            This function creates a new feild in a talbe of the data base
            NOTE: This function is for other threads to call!
            
            Inputs:
                args[0] : dict of new table to make
        '''

        for key in args[0]:
            table_name = key
            new_data_type = dataType(table_name, self.__coms)
            for feilds_list in args[0][key]:
                if 'list ' not in feilds_list[1]:
                    new_data_type.add_feild(feilds_list[0], 0, feilds_list[1])
                else :
                    new_data_type.add_feild('index_internal', 0, 'int')
                    new_data_type.add_feild(feilds_list[0], 0, feilds_list[1].replace('list ', ''))
                    new_data_type.add_feild('Data_group', 0, 'string')
                
            self.__tables[table_name] = new_data_type

            self.create_table([table_name]) #add the table
            self.create_feilds_archived([table_name])     
    def create_feilds_archived(self, args):
        '''
            This function creates an archived in the data base for all the 
            mappings.

            ARGS:
                args[0] : table structure name to be archived
        '''

        table_name = self.__tables[args[0]].get_data_group()
        table_feilds = self.__tables[args[0]].get_fields()
        #Open the archived file
        self.__dataFile = open("database/dataTypes.dtobj", 'a') # pylint: disable=r1732
        self.__dataFile.write(table_name + "\n")
        for feild in table_feilds:
            feild_info = self.__tables[table_name].get_field_info(feild)
            self.__dataFile.write(f"    {feild}:{feild_info[0]} > {feild_info[1]}\n")
        self.__dataFile.close()
        #Open the archived back up file
        self.__dataFile = open("database/dataTypes_backup.dtobj", 'a') # pylint: disable=r1732
        self.__dataFile.write(table_name + "\n")
        for feild in table_feilds:
            feild_info = self.__tables[table_name].get_field_info(feild)
            self.__dataFile.write(f"    {feild}:{feild_info[0]} > {feild_info[1]}\n")
        self.__dataFile.close()
    def save_data_group(self, args):
        '''
            This function takes in a list of data to store as a group

            ARGS:
                args[0] : table name
                args[1] : dict of data to store
        '''

        start_time = time.time()

        #get the index
        self.__c.execute(f"SELECT * FROM {args[0]} WHERE table_idx = (SELECT max(table_idx) FROM {args[0]})")
        row = pd.DataFrame(self.__c.fetchall()) 
        if row.empty:
            idx = 0
        else :
            idx  = row[0][0] + 1
        
        key = next(iter(args[1])) #get the key of the first index 
        data_length = len(args[1][key]) #get the length of the expected data 

        for i in range(data_length): #for all the feilds loop throught hte data and get it added to a list to be inserted. 
            data_list = []
            for feild in args[1]:
                data = args[1][feild][i]
                try:
                    if(isinstance(data, str)): data_list.append(data) #strings can be index but we want to save the whole thing. Thats why this line is here.
                    else : data_list.append(data[0]) #sometimes matlab returns things like matlab.double witch you need to index to actuall get the data
                except :
                    data_list.append(data)            
            self.insert_data([args[0], data_list], idx)
            idx += 1 # incrament the data base index.
        self.__conn.commit() #this line commits the feilds to the data base.
        self.__coms.print_message(f"Inserted Data time {time.time() - start_time}.")
    def get_data_large(self, args):
        '''
            args is a list where the fist index is the table name 
            and the second is the start time for collecting the data
            ARGS:
                [0] table name
                [1] table_idx (starting indx)
            RETURNS:
                pandas data obj
            NOTE: This function IS for larg amouts of data! 
        '''
        message = f"<h1>{args[0]}: " 
        try :
            #from and run db command
            db_command = f"SELECT * FROM {args[0]} WHERE table_idx >= {str(args[1])} ORDER BY table_idx"
            self.__c.execute(db_command)
            self.__logger.send_log("Query command recived: "  + db_command)
            self.__coms.print_message("Query command recived: "  + db_command, 2)
        except Exception as error: # pylint: disable=w0718
            self.__coms.print_message(str(error) + " Command send to db: " + db_command, 0)
            self.__logger.send_log(str(error) + " Command send to db: " + db_command)
            return "<p> Error getting data </p>"
        #get cols 
        cols = ["Table Index "] # add table_idx to the cols lis
        cols += self.get_feilds_list([self.get_data_type([args[0]])])
        message += f"{cols}</h1> "
        #fetch and convert the data into a pandas data frame.
        data = pd.DataFrame(self.__c.fetchall(), columns=cols)  
        return data
