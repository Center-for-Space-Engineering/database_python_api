'''
    This class turns defined data types into python class that can be used by the data
    base and other classes in the system so that we can do data elevation.
'''
from database_python_api.dataType import dataType # pylint: disable=e0401
from logging_system_display_python_api.logger import loggerCustom # pylint: disable=e0401


class dataTypeImporter():
    '''
        This class is tasked with translating a user defined data type into python classes. 
        RIGHT NOW: it uses text files.
        TODO: IT needs to use the exle dictionary files. 
    '''
    def __init__(self, coms=None):
        self.__data_types = {}
        self.__logger = loggerCustom("logs/dataTypeImporter.txt")
        self.__coms = coms
        try:
            self.__dataFile = open("database/dataTypes.dtobj") # pylint: disable=r1732
            self.__logger.send_log("data types file found.")
            self.__coms.print_message("data types file found.", 2)
        except: # pylint: disable=w0702
            self.__coms.print_message(" No databasei/dataTypes.dtobj file detected!", 0)   
            self.__logger.send_log(" No database/dataTypes.dtobj file detected!")   
    def pasre_data_types(self):
        '''
            reads file and then creates the data type classes based on what the file says. 
        '''
        current_data_group = ""
        for line in self.__dataFile:
            if "//" in line:
                pass
            else :
                if ('    ' in line) or ('\t' in line):
                    if '@' in line: # this is a discontinuos type
                        processed = line.replace('  ', "")
                        processed = processed.replace("\n", "")
                        processed = processed.split(":")
                        temp = processed[1].split(">")
                        processed[1] = temp[0]
                        feild = temp[1].split("@")
                        processed.append(feild[0])
                        disCon = feild[1].split('<')

                        self.__logger.send_log(f"Decoded type for group {current_data_group} : feild name {processed[0].strip()}, bit length {processed[1].strip()}, convertion typ {processed[2].strip()}")
                        self.__data_types[current_data_group].add_feild(processed[0].strip(), processed[1].strip(), processed[2].strip())
                        #because this is a discontinuos type we need to add it to the list of discontinuos types
                        self.__data_types[current_data_group].add_conver_map(disCon[0], disCon[1])
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
                        
                        self.__logger.send_log(f"Decoded type for group {current_data_group} : feild name {processed[0].strip()}, bit length {processed[1].strip()}, convertion typ {processed[2].strip()}")
                        self.__data_types[current_data_group].add_feild(processed[0].strip(), processed[1].strip(), processed[2].strip())
                elif "#" in line:
                    processed = line.replace('  ', "")
                    processed = processed.replace("\n", "")
                    processed = processed.split(":")
                    self.__logger.send_log(f"Decoded type for group {current_data_group} : feild name ignored bits, bit length {processed[1]}")
                    self.__data_types[current_data_group].add_feild("igrnoed feild", processed[1].strip(), "NONE")
                else :
                    processed = line.replace('  ', "")
                    processed = processed.replace("\n", "")
                    current_data_group = processed.strip()
                    self.__data_types[current_data_group] = dataType(current_data_group, self.__coms)
                    self.__logger.send_log(f"Created data group {current_data_group}")

        self.__logger.send_log(f"Created data types:\n {self}")   
        self.__coms.print_message("Created data types.", 2)   
    def get_data_types(self):
        # pylint: disable=missing-function-docstring
        return self.__data_types
    def __str__(self):
        # pylint: disable=missing-function-docstring
        message = ""
        for group in self.__data_types: # pylint: disable=c0206
            message += str(self.__data_types[group]) + "\n"

        return message
