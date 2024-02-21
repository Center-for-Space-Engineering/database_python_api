'''
  This module handles the database. It takes in commands and then translates them into sql
  then passes that onto the data base. 
'''
import sqlite3
import time
import pandas as pd

from database_python_api.dataTypesImporter import dataTypeImporter # pylint: disable=e0401
from database_python_api.dataType import dataType # pylint: disable=e0401
from logging_system_display_python_api.logger import loggerCustom # pylint: disable=e0401
from threading_python_api.threadWrapper import threadWrapper # pylint: disable=e0401

#import DTO for communicating internally
from DTOs.print_message_dto import print_message_dto # pylint: disable=e0401



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
    def __init__(self, coms, db_name = 'database/database_file', is_gui = False):
        #make class vars
        self.__logger = loggerCustom("logs/database_log_file.txt")
        self.__coms = coms
        self.__db_name = db_name
        self.__conn = None
        self.__c = None
        self.__tables = None
        self.__is_gui = is_gui

        #make Maps for db creation
        self.__type_map = { #the point of this dictionary is to map the type names from the
                            # dataTypes.dtobj file to the sql data base.
            "int" : "INTEGER", 
            "float" : "FLOAT(10)", # NOTE: the (#) is the precision of the float. 
            "string" : "TEXT",
            "bool" : "BOOLEAN",
            "bigint" : "BIGINT", 
            "byte" : "BLOB",
        } #  NOTE: this dict makes the .dtobj file syntax match sqlite3 syntax.

        #Start the thread wrapper for  the process
        self.__function_dict = {
            'create_data_base' : self.create_data_base,
            'create_table' : self.create_table,
            'insert_data' : self.insert_data,
            'get_tables_html' : self.get_tables_html,
            'get_tables_str_list' : self.get_tables_str_list,
            'get_data_type' : self.get_data_type,
            'get_fields' : self.get_fields,
            'get_fields_list' : self.get_fields_list,
            'get_data' : self.get_data,
            'create_table_external' : self.create_table_external,
            'create_fields_archived' : self.create_fields_archived,
            'save_data_group' : self.save_data_group,
            'get_data_large' : self.get_data_large,
            'save_byte_data' : self.save_byte_data
        }
        super().__init__(self.__function_dict)

        #send request to parent class to make the data base
        super().make_request('create_data_base', [])
        self.__dataFile = None
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
        tableFinder.parse_data_types()
        self.__tables = tableFinder.get_data_types() #this variable will be used for creating/accessing/parsing the data. 
        #  In other words its super important.  

        #create a table in the data bas for each data type
        for table in self.__tables:
            self.create_table([table])
        self.__logger.send_log("Created database:\n" + self.get_tables_html())
        dto = print_message_dto("Created database")
        self.__coms.print_message(dto, 2)   
    def create_table(self, args):
        '''
            This function looks to create a table. If the table already exists if will not create it. 

            NOTE: This function is NOT for other threads to call! Use the create_table_external func.
            args[0] : is the table name
        '''
        table_name = self.__tables[args[0]].get_data_group()
        table_fields = self.__tables[args[0]].get_fields()

        db_command = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        #this line is add as a way to index the data base
        db_command += "[table_idx] BOOLEAN PRIMARY KEY" 

        #iter for every field in this data group. The fields come from the dataTypes.dtobj file. 
        for field_name in table_fields:
            # NOTE: in the dataTypes class the fields dict maps to a tuple of (bits, convert_type)
            # the bits is not used here because it is for collecting the data. The convert typ is,
            # thus we want to access field 1 of the tuple. In addition that is fed into the 
            # self.__type_map to convert it to SQL syntax
            #we dont need fields for ignored data fields
            if field_name!= "ignored field" and table_fields[field_name][1] != "NONE":
                # adding the ", " here means we don't have an extra one on the last line.
                db_command += ", "
                if table_fields[field_name][1] != 'byte':
                    db_command += f"[{field_name}] {self.__type_map[table_fields[field_name][1]]}"
                #if it is a byte type we need to set the size of the field
                else :
                    db_command += f"[{field_name}] {self.__type_map[table_fields[field_name][1]]}({self.__tables[args[0]].get_field_info(field_name)[0]})" # this self.__tables[args[0]].get_field_info(field_name)[0]} gets the bit length out of the data type class
        db_command += ")"

        #try to make the table in the data base
        try :  
            self.__c.execute(db_command)
            self.__logger.send_log("Created table: " + db_command)
            dto = print_message_dto("Created table: " + db_command)
            self.__coms.print_message(dto, 2)
        except Exception as error: # pylint: disable=w0718
            dto = print_message_dto(str(error) + " Command send to db: " + db_command)
            self.__coms.print_message(dto, 0) 
            self.__logger.send_log("Failed to created table: " + db_command + str(error))
            dto = print_message_dto("Failed to created table: " + db_command + str(error))
            self.__coms.print_message(dto, 0)
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
        #get the data type obj and then get the fields list
        fields = self.get_fields([self.get_data_type([args[0]])]) 
        for field_name in fields:
            #we dont need fields for ignored data fields
            if field_name!= "ignored field" and fields[field_name][1] != "NONE" : 
                db_command += ", "
                db_command += f"{field_name}"
        db_command +=  ") "
        db_command += f"VALUES ({idx}"

        for val in args[1]: #args[1] is the data
            db_command += ", "
            # for sql str has a special format so we need the if statement

            if not isinstance(val, str):
                db_command += f"{val}"
            else :
                db_command += f"'{val}'" #Notice that there is '' on this line.           
        db_command += ");"
        try:
            self.__c.execute(db_command)
            self.__logger.send_log(" Insert data command sent to database. ")
        except Exception as error: # pylint: disable=w0718
            dto = print_message_dto(str(error) + " Command send to db: " + db_command)
            self.__coms.print_message(dto, 0) 
            self.__logger.send_log(str(error) + " Command send to db: " + db_command)

            if 'UNIQUE constraint failed' in str(error):
                dto = print_message_dto(f" Duplicate  time stamp {idx}")
                self.__coms.print_message(dto) 
                self.__logger.send_log(f" Duplicate  time stamp {idx}")
            else :
                # pylint: disable=w0707
                # pylint: disable=w0719
                raise Exception
        return "Complete"
    #some  useful getters
    def get_tables_html(self):
        # pylint: disable=missing-function-docstring
        message = "<! DOCTYPE html>\n<html>\n<body>\n<h1>DataBase Tables</h1>"
        for table in self.__tables: 
            message +=f"<table_name>Table: {table}</table_name><p></p>" # pylint: disable=r1713
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
    def get_fields(self, args):
        '''
            args is a list where the fist index is a data type obj 
        '''
        return args[0].get_fields() # the args is a data type obj in the first index of the list
    def get_fields_list(self, args):
        '''
            args is a list where the fist index is a data type obj 
        '''
        fields = args[0].get_fields()# the args is a data type obj in the first index of the list
        fields_list = []
        for field in fields:
            if fields[field][1] != "NONE": #dont add any fields that are not in the data base
                fields_list.append(field)
        return fields_list
    def get_data(self, args):
        '''
            args is a list where the fist index is the table name 
            and the second is the start time for collecting the data
            ARGS:
                [0] table name
                [1] table_idx (starting index)
            RETURNS:
                html string with data
            NOTE: This function is NOT meant for large amounts of data! Make request small request to this function of it will take a long time. 
        '''
        message = f"<h1>{args[0]}: " 
        try :
            #from and run db command
            db_command = f"SELECT * FROM {args[0]} WHERE table_idx >= {str(args[1])} ORDER BY table_idx"
            self.__c.execute(db_command)
            self.__logger.send_log("Query command received: "  + db_command)
            dto = print_message_dto("Query command received: "  + db_command)
            self.__coms.print_message(dto, 2)
        except Exception as error: # pylint: disable=w0718
            dto = print_message_dto(str(error) + " Command send to db: " + db_command)
            self.__coms.print_message(dto, 0)
            self.__logger.send_log(str(error) + " Command send to db: " + db_command)
            return "<p> Error getting data </p>"
        #get cols 
        cols = ["Table Index"] # add table_idx to the cols lis
        cols += self.get_fields_list([self.get_data_type([args[0]])])
        message += f"{cols}</h1> "
        #fetch and convert the data into a pandas data frame.
        data = pd.DataFrame(self.__c.fetchall(), columns=cols)
        for idx in range(len(data)):
            message += "<p>"
            for i in range(len(cols) - 1): #iterate for all but last col
                message += f"{data.iloc[idx,i]},"
                message += f"{data.iloc[idx,len(cols) - 1]}"# add last col with out ,
            message += "</p>"
        self.__logger.send_log("data collected for server. (get_data_command)")
        return message
        #this is the setter section
    #Functions for DB control (Usually through requests made by other threads)
    def create_table_external(self, args):
        '''
            This function creates a new field in a table of the data base
            NOTE: This function is for other threads to call!
            
            Inputs:
                args[0] : dict of new table to make
        '''

        for key in args[0]:
            table_name = key
            new_data_type = dataType(table_name, self.__coms, idx_name=args[0][key][0][1]) #access the dictionary for the current table then access the first list then access first member of the list with is our idx name. 
            input_idx = None
            for fields_list in args[0][key]:
                if not ('input_idx_db' in fields_list[0]): #pylint: disable=c0325
                    new_data_type.add_field(fields_list[0], 0, fields_list[1])
                else : 
                    input_idx = fields_list
                
            self.__tables[table_name] = new_data_type

            self.create_table([table_name]) #add the table
            self.create_fields_archived([table_name, input_idx])     
    def create_fields_archived(self, args):
        '''
            This function creates an archived in the data base for all the 
            mappings.

            ARGS:
                args[0] : table structure name to be archived
                args[1] : input idx name 
        '''

        table_name = self.__tables[args[0]].get_data_group()
        table_fields = self.__tables[args[0]].get_fields()
        #Open the archived file
        self.__dataFile = open("database/dataTypes.dtobj", 'a') # pylint: disable=r1732
        self.__dataFile.write(table_name + "\n")
        if args[1] is not None:
            self.__dataFile.write(f"    {args[1][0]}:{args[1][1]}\n")
        for field in table_fields:
            field_info = self.__tables[table_name].get_field_info(field)
            self.__dataFile.write(f"    {field}:{field_info[0]} > {field_info[1]}\n")
        self.__dataFile.close()
        #Open the archived back up file
        self.__dataFile = open("database/dataTypes_backup.dtobj", 'a') # pylint: disable=r1732
        self.__dataFile.write(table_name + "\n")
        if args[1] is not None:
            self.__dataFile.write(f"    {args[1][0]}:{args[1][1]}\n")
        for field in table_fields:
            field_info = self.__tables[table_name].get_field_info(field)
            self.__dataFile.write(f"    {field}:{field_info[0]} > {field_info[1]}\n")
        self.__dataFile.close()
    def save_data_group(self, args):
        '''
            This function takes in a group of data to store as a group

            ARGS:
                args[0] : table name
                args[1] : dict of data to store
                args[2] : thread id (used for reporting)
        '''

        start_time = time.time()

        #get the index
        self.__c.execute(f"SELECT * FROM {args[0]} WHERE table_idx = (SELECT max(table_idx) FROM {args[0]})")
        row = pd.DataFrame(self.__c.fetchall()) 
        thread_name = args[2]
        if row.empty:
            idx = 0
        else :
            idx  = row[0][0] + 1
        
        key = next(iter(args[1])) #get the key of the first index 
        data_length = len(args[1][key]) #get the length of the expected data 
        idx_field_name = self.get_data_type([args[0]]).get_idx_name()

        for i in range(data_length): #for all the fields loop thought hte data and get it added to a list to be inserted. 
            data_list = []
            try:
                idx = args[1][idx_field_name][i][0] #if we have a time stamp lets use that as our index
            except : # pylint: disable=w0702
                pass #no timestamp given
            for field in args[1]:
                if field != idx_field_name:
                    if args[1][field][i] == 'NaN':
                        data = 0
                    else : data = args[1][field][i]
                    try:
                        if isinstance(data, str):
                            data_list.append(data) #strings can be index but we want to save the whole thing. Thats why this line is here.
                        else :
                            data_list.append(data[0]) #sometimes matlab returns things like matlab.double witch you need to index to actual get the data
                    except : # pylint: disable=w0702
                        data_list.append(data)            
            self.insert_data([args[0], data_list], idx)
            if self.__is_gui :
                self.__coms.send_request('Gui handler (SysEmuo)', ['make_save_report', thread_name, ((i + 1) / data_length) * 100])
            idx += 1 # increment the data base index.
        self.__conn.commit() #this line commits the fields to the data base.
        dto = print_message_dto(f"Inserted Data time: {time.time() - start_time}.")
        self.__coms.print_message(dto)
    def get_data_large(self, args):
        '''
            args is a list where the fist index is the table name 
            and the second is the start time for collecting the data
            ARGS:
                [0] table name
                [1] table_idx (starting index)
                [2] Max rows allowed to be fetched at one time
            RETURNS:
                pandas data obj
            NOTE: This function IS for large amounts of data! 
        '''
        try :
            #from and run db command
            max_rows = 0
            have_max = False
            try :
                max_rows = args[2]
                have_max = True
            except : # pylint: disable=w0702
                pass
            if have_max :
                db_command = f"SELECT * FROM {args[0]} WHERE table_idx >= {str(args[1])} ORDER BY table_idx LIMIT  {max_rows}"
            else :
                db_command = f"SELECT * FROM {args[0]} WHERE table_idx >= {str(args[1])} ORDER BY table_idx"
            self.__c.execute(db_command)
            self.__logger.send_log("Query command received: "  + db_command)
            dto = print_message_dto("Query command received: "  + db_command)
            self.__coms.print_message(dto, 2)
        except Exception as error: # pylint: disable=w0718
            dto = print_message_dto(str(error) + " Command send to db: " + db_command)
            self.__coms.print_message(dto, 0)
            self.__logger.send_log(str(error) + " Command send to db: " + db_command)
            return "<p> Error getting data </p>"
        #get cols 
        cols = ["Table Index"] # add table_idx to the cols list
        cols += self.get_fields_list([self.get_data_type([args[0]])])
        #fetch and convert the data into a pandas data frame.
        data = pd.DataFrame(self.__c.fetchall(), columns=cols)  
        return data
    def save_byte_data(self, args):
        '''
            This function is in charge of saving byte data (BLOB)

            NOTE: This is not a general save like the insert data, it is use case specific. 

            args:
                [0] : table name
                [1] : list of bytes
                [2] : caller thread name
        '''
        start_time = time.time()

        #get the index
        self.__c.execute(f"SELECT * FROM {args[0]} WHERE table_idx = (SELECT max(table_idx) FROM {args[0]})")
        row = pd.DataFrame(self.__c.fetchall()) 
        _ = args[2] # this is the thread name not used right now but might be used one day. 
        if row.empty:
            idx = 0
        else :
            idx  = row[0][0] + 1
        
        key = next(iter(args[1])) #get the key of the first index 
        data_length = len(args[1][key]) #get the length of the expected data

        for i in range(data_length): #for all the fields loop thought the data and get it added to a list to be inserted.
            db_command = f"INSERT INTO {args[0]} (table_idx"
            #get the data type obj and then get the fields list
            fields = self.get_fields([self.get_data_type([args[0]])]) 
            for field_name in fields:
                #we dont need fields for ignored data fields
                if field_name!= "ignored field" and fields[field_name][1] != "NONE" : #pylint disable=w0718
                    db_command += ", "
                    db_command += f"{field_name}"
            db_command +=  ") "
            # byte_str_temp = ''.join(format(x, '02x') for x in args[1][key][i])
            # print(f"raw: {args[1][key][i]}\n old conversion: {byte_str_temp}")
            db_command += f"VALUES ({idx}, ?)" # this self.__tables[args[0]].get_field_info(field_name)[0]} gets the bit length out of the data type class

            try:
                self.__c.execute(db_command, (args[1][key][i],))
                self.__logger.send_log(" Insert command (byte) sent to data base ")
            except Exception as error: # pylint: disable=w0718
                dto = print_message_dto(str(error) + " Command send to db: " + db_command)
                self.__coms.print_message(dto, 0) 
                self.__logger.send_log(str(error) + " Command send to db: " + db_command)

                if 'UNIQUE constraint failed' in str(error):
                    dto = print_message_dto(f" Duplicate  time stamp {idx}")
                    self.__coms.print_message(dto)
                    self.__logger.send_log(f" Duplicate  time stamp {idx}")
                else :
                    # pylint: disable=w0707
                    # pylint: disable=w0719
                    raise Exception
            idx += 1 # increment the data base index.
        self.__conn.commit() #this line commits the fields to the data base.
        dto = print_message_dto(f"Inserted Data time: {time.time() - start_time}.")
        self.__coms.print_message(dto)
