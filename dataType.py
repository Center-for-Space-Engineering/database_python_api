'''
    This module is how we are going to convert any data time to any other data type, including elevating it from the bit stream to L 1/2.
'''

from logging_system_display_python_api.logger import loggerCustom # pylint: disable=e0401


class dataType():
    '''
        This class contains all the necessary mappings for class so that we can convert data from one type to another type.
    '''
    def __init__(self, dataGroup, coms = None, idx_name = ''):
        self.__fields = {} #this dict contains all the data types that will be saved to the data base
        self.__bit_map = []# this list contains info on how to collect the bits from the bit stream. 
        self.__convert_map = {} #this dict contains types that need to be mapped together. The MSB is the key.  
        self.__data_group = dataGroup
        self.__logger = loggerCustom(f"logs/dataType_{self.__data_group}.txt") 
        _ = coms  # this will be self.__coms one day just not using it right now
        self.__idx_name = idx_name

    def add_field(self, name, bits, convert):
        '''
            Adds a field that can be converted
        '''
        self.__fields[name] = (bits, convert)
        self.__logger.send_log(f"{self.__data_group} added a field: {name} : bit length {bits} > converter type {convert}")
        self.add_bit_map(name, bits) # add the type to the bit map     
    def add_bit_map(self, name, bits):    
        '''
            tells this class where to find the bits it needs
        '''
        self.__bit_map.append((name, bits))
        self.__logger.send_log(f"{self.__data_group} added a bit map step: {name} : bit length {bits}")
    def add_convert_map(self, type1, type2):
        '''
            add a map for combining types together. A.K.A field1, field2 = field3
        '''
        self.__convert_map[type1] = type2
        self.__logger.send_log(f"{self.__data_group} added a discontiunous data type: {type1} < {type2}")
    def __str__(self):
        # pylint: disable=missing-function-docstring
        message = f"<! DOCTYPE html>\n<html>\n<body>\n<h1><strong>Data Type:</strong> {self.__data_group}</h1>\n"
        message += "<h1><strong>Feilds in database:</strong></h1>\n"

        for field in self.__fields: # pylint: disable=c0206
            if self.__fields[field][1] != "NONE":
                message +=f"<p>&emsp;<strong>Field:</strong> {field} <strong>Bit length:</strong> {self.__fields[field][0]} <strong>Converter type:</strong> {self.__fields[field][1]} </p>\n"
        message += "<h2><strong>Bit Map:</strong></h2>\n"
        for map_iter in self.__bit_map:
            message +=f"<p>&emsp;<strong>Field:</strong> {map_iter[0]} <strong>Bit length:</strong> {map_iter[1]} </p>\n"
        message += "<h3><strong>Discontinuous Mappings:</strong></h3>\n"
        for typeC in self.__convert_map: # pylint: disable=c0206
            message +=f"<p>&emsp;<strong>Field MSB:</strong> {typeC} <strong>Field LSB</strong> {self.__convert_map[typeC]} </p>\n"
        message += "</body>\n</html>"
        return message
    def get_fields(self):
        # pylint: disable=missing-function-docstring
        return self.__fields  
    def get_data_group(self):
        # pylint: disable=missing-function-docstring
        return self.__data_group
    def get_field_info(self, field_name):
        # pylint: disable=missing-function-docstring
        return self.__fields[field_name]
    def get_idx_name(self):
        # pylint: disable=missing-function-docstring
        return self.__idx_name
    def set_idx_name(self, name):
        # pylint: disable=missing-function-docstring
        self.__idx_name = name
