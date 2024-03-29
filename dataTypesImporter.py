'''
    This class turns defined data types into python class that can be used by the data
    base and other classes in the system so that we can do data elevation.
'''
from database_python_api.dataType import dataType # pylint: disable=e0401
from logging_system_display_python_api.logger import loggerCustom # pylint: disable=e0401

#import DTO for communicating internally
from logging_system_display_python_api.DTOs.print_message_dto import print_message_dto # pylint: disable=e0401

class dataTypeImporter():
    '''
        This class is tasked with translating a user defined data type into python classes. 
    '''
    def __init__(self, coms=None):
        self.__data_types = {}
        self.__logger = loggerCustom("logs/dataTypeImporter.txt")
        self.__coms = coms
        try:
            self.__dataFile = open("database/dataTypes.dtobj") # pylint: disable=r1732
            self.__logger.send_log("data types file found.")
            dto = print_message_dto("data types file found.")
            self.__coms.print_message(dto, 2)
        except: # pylint: disable=w0702
            dto = print_message_dto(" No database/dataTypes.dtobj file detected!")
            self.__coms.print_message(dto, 0)   
            self.__logger.send_log(" No database/dataTypes.dtobj file detected!")   
    def parse_data_types(self):
        '''
            reads file and then creates the data type classes based on what the file says. 
        '''
        current_data_group = ""
        for line in self.__dataFile:
            if "//" in line:
                pass
            else :
                if ('    ' in line) or ('\t' in line):
                    if '@' in line: # this is a discontinues type
                        processed = line.replace('  ', "")
                        processed = processed.replace("\n", "")
                        processed = processed.split(":")
                        temp = processed[1].split(">")
                        processed[1] = temp[0]
                        field = temp[1].split("@")
                        processed.append(field[0])
                        disCon = field[1].split('<')

                        self.__logger.send_log(f"Decoded type for group {current_data_group} : field name {processed[0].strip()}, bit length {processed[1].strip()}, conversion typ {processed[2].strip()}")
                        self.__data_types[current_data_group].add_field(processed[0].strip(), processed[1].strip(), processed[2].strip())
                        #because this is a discontinues type we need to add it to the list of discontinues types
                        self.__data_types[current_data_group].add_convert_map(disCon[0], disCon[1])
                    elif 'input_idx_db' in line:
                        line = line.replace("\n", "")
                        processed = line.split(":")
                        self.__data_types[current_data_group].set_idx_name(processed[1]) #add the input_idx_db to the data type so that the data base knows what to use as its index.
                    else :
                        processed = line.replace('  ', "")
                        processed = processed.replace("\n", "")
                        processed = processed.split(":")
                        temp = processed[1].split(">")
                        processed[1] = temp[0]
                        if len(temp) > 1:
                            processed.append(temp[1])
                        else :
                            processed.append("NONE")
                        
                        self.__logger.send_log(f"Decoded type for group {current_data_group} : field name {processed[0].strip()}, bit length {processed[1].strip()}, conversion typ {processed[2].strip()}")
                        self.__data_types[current_data_group].add_field(processed[0].strip(), processed[1].strip(), processed[2].strip())
                elif "#" in line:
                    processed = line.replace('  ', "")
                    processed = processed.replace("\n", "")
                    processed = processed.split(":")
                    self.__logger.send_log(f"Decoded type for group {current_data_group} : field name ignored bits, bit length {processed[1]}")
                    self.__data_types[current_data_group].add_field("ignored field", processed[1].strip(), "NONE")
                else :
                    processed = line.replace('  ', "")
                    processed = processed.replace("\n", "")
                    current_data_group = processed.strip()
                    if len(current_data_group) > 0: # Make sure this is not a empty line.
                        self.__data_types[current_data_group] = dataType(current_data_group, self.__coms)
                        self.__logger.send_log(f"Created data group {current_data_group}")

        self.__logger.send_log(f"Created data types:\n {self}")
        dto = print_message_dto("Created data types.")  
        self.__coms.print_message(dto, 2)   
    def get_data_types(self):
        # pylint: disable=missing-function-docstring
        return self.__data_types
    def __str__(self):
        # pylint: disable=missing-function-docstring
        message = ""
        for group in self.__data_types: # pylint: disable=c0206
            message += str(self.__data_types[group]) + "\n"

        return message
