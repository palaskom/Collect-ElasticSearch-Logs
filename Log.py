import copy
from datetime import datetime
from common_functions import *

class Log:

    def __init__(self, *args) -> None:
        self.version = None
        self.timestamp = None
        self.service_id = None
        self.severity = None
        self.message = None
        if len(args) == 5:
            self.init_explicit(args)
        elif len(args) == 1:
            if type(args) == dict:
                self.init_dict(args)
            elif type(args) == str:
                self.init_str(args)
            else:
                exit_and_fail("The input data must be either a string or a dictionary")
        else:
            exit_and_fail("All mandatory log fields must be provided as: 5 separate variables, 1 dictionary, or 1 string")


    def init_explicit(self, data:list) -> None:
        self.version = data[0]
        self.timestamp = self.formulate_timestamp(data[1])
        self.service_id = data[2]
        self.severity = data[3]
        self.message = data[4]
    
    def init_dict(self, data:dict) -> None:
        if "version" in data.keys(): 
            self.version = data['version']
        if "service_id" in data.keys(): 
            self.service_id = data['service_id']
        if "severity" in data.keys(): 
            self.severity = data['severity']
        if "message" in data.keys(): 
            self.message = data['message']
        if "timestamp" in data.keys(): 
            self.timestamp = self.formulate_timestamp(data['timestamp'])

    def init_str(self, data:str) -> None:
        if is_json(data):
            self.init_dict(convert_json_to_dict(data))
        else:
            self.message = data[:len(log)-2] # because log's format is "bla-bla-bla\n"


    def formulate_timestamp(self, timestamp):
        '''
        Transforms the provide log timestamp from the given value to a python datetime object.
        This procedure can be successfully completed only if the timestamp is a string of the 
        form %Y-%m-%dT%H:%M:%S.%f%z e.g. "2022-11-14T08:33:43.288686+00:00" or 
        "2022-11-14T08:33:43.288686+0000" or "2022-11-14T08:33:43.288686Z". If 
        the provided timestamp is not a string or it is a string that cannot be converted to 
        a datetime object, it is considered invalid. 
        '''
        if type(timestamp) != str:
            return timestamp
        try:
            td = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
        except ValueError:
            td = timestamp
        return td


    def is_timestamp_valid(self) -> bool:
        if (self.timestamp != None) and (type(self.timestamp)==datetime):
            return True
        return False

    def is_version_valid(self) -> bool:
        if (self.version != None) and (type(self.version) == str): 
            if self.version in ["0.2.0", "0.3.0", "1.0.0", "1.1.0", "1.2.0"]:
                return True
        return False

    def is_severity_valid(self) -> bool:
        if (self.severity != None) and (type(self.severity) == str):
            if self.severity in ["debug", "info", "warning", "error", "critical"]:
                return True
        return False

    def is_serviceId_valid(self) -> bool:
        if (self.service_id != None):
            if (type(self.service_id) == str) and len(self.service_id) >0:
                return True
        return False

    def is_message_valid(self) -> bool:
        if (self.message != None) and (type(self.message) == str):
            return True
        return False
               
    def is_valid(self) -> bool:
        if self.is_version_valid() and self.is_serviceId_valid() and self.is_severity_valid() and self.is_timestamp_valid() and self.is_message_valid():
            return True
        return False
           

    def convert_to_dict(self) -> dict:
        '''
        Converts a Log object to a dictionary
        Note: timestamp is a datetime object, we have to convert it to a string so that it can be
        properly displayed when using json.dumps to write the dictionary to a file
        '''
        Log_dict = vars(copy.deepcopy(self)) # vars(obj) converts a class object to a dictionary
        if type(Log_dict['timestamp']) == datetime:
            Log_dict['timestamp'] = str(Log_dict['timestamp'].isoformat('T')) 
        return Log_dict


    def get_str_datetime(self) -> str:
        return str(self.timestamp.date()) + "T" + str(self.timestamp.time()).split('.')[0]