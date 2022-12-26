from Log import *
from common_functions import *
from datetime import timedelta


class Container:
    
    def __init__(self, service_id=None, pod_name=None, container_name=None, logFile=None):
        self.name = container_name
        self.service_id = service_id
        self.pod_name = pod_name
        self.previous_logs = [] # logs before restart -> list of Log objects
        self.logs = [] # logs after restart -> list of Log objects
        self.logFiles = []
        self.resultsFolder = None
        self.resultsExcel = None
        if logFile != None: self.logFiles.append(logFile)
            

    def fetch_logs(self, folder_path:str):
        if len(self.logFiles) == 0:
            warning(f"No logs were fetched for the '{self.name}' container of the '{self.pod_name}' pod")
        else:
            for logFile in self.logFiles:
                logs = read_logs_from_file(folder_path + logFile, "line")
                for log in logs:
                    log_obj = Log(log)
                    self.logs.append(log_obj)
                    if "previous" in logFile:
                        self.previous_logs.append(log_obj)


    def contains_logs(self) -> bool:
        if len(self.logs) > 0:
            return True
        return False


    def add_log(self, version=None, timestamp=None, service_id=None, severity=None, message=None):
        new_log = Log(version, timestamp, service_id, severity, message)
        self.logs.append(new_log)


    def get_folder(self, parent=None):
        folder = self.service_id + "/" + self.pod_name + "/" + self.name + "/"
        if parent == None:
            return folder
        return parent + "/" + folder


    def create_folder(self, parent):
        create_folder(parent, self.service_id)            
        service_folder = parent + self.service_id + "/"
        create_folder(service_folder, self.pod_name)
        pod_folder = service_folder + self.pod_name + "/"
        create_folder(pod_folder, self.name)


    def separate_logs(self):
        valid_logs = []
        multiline_logs = []
        no_json_logs = []
        missing_fields_logs = []
        for log in self.logs:
            if log.is_valid(): 
                valid_logs.append(log)
            else:
                if log.message.startswith('\tat') or log.message.startswith('io') or log.message.startswith('Caused by'):
                    multiline_logs.append(log)
                elif log.is_message_valid() and log.version==None and log.severity==None and log.service_id==None and log.timestamp==None:
                    no_json_logs.append(log)
                else:
                    missing_fields_logs.append(log)
        return valid_logs, multiline_logs, no_json_logs, missing_fields_logs


    def separate_SE_logs(self):
        valid_logs = []
        multiline_logs = []
        no_json_logs = []
        invalid_fields_logs = []
        for log in self.logs:
            if log.is_valid():
                if log.version in ["1.0.0", "1.1.0", "1.2.0"]: 
                    valid_logs.append(log)
                else:
                    if log.message.startswith('\tat') or log.message.startswith('io') or log.message.startswith('Caused by'):
                        multiline_logs.append(log)
                    else:
                        no_json_logs.append(log)
            else:
                invalid_fields_logs.append(log)
        return valid_logs, multiline_logs, no_json_logs, invalid_fields_logs


    def get_first_valid_timestamp_log(self):
        for log in self.logs():
            if log.is_timestamp_valid(): 
                return log
        return None

    def get_last_valid_timestamp_log(self):
        for log in reversed(self.logs()):
            if log.is_timestamp_valid(): 
                return log
        return None

    def get_datetime_period(self):
        logs_number = len(self.logs())
        if logs_number == 0:
            return None
        first_valid_timestamp_log = self.get_first_valid_timestamp_log()
        if first_valid_timestamp_log==None: 
            return "no valid timestamps"
        last_valid_timestamp_log = self.get_last_valid_timestamp_log()
        return first_valid_timestamp_log.get_str_datetime() + " - " + last_valid_timestamp_log.get_str_datetime()


    def get_logs_per_period(self):
        period_timestamps = []
        last_valid_timestamp_log = self.get_last_valid_timestamp_log()
        if last_valid_timestamp_log==None:
            return ["None", "None"], [self.logs()]
        last_valid_td = last_valid_timestamp_log.timestamp
        first_valid_timestamp_log = self.get_first_valid_timestamp_log()
        current_td_sec = first_valid_timestamp_log.timestamp
        while True:
            if current_td_sec >= last_valid_td:
                period_timestamps.append(current_td_sec)
                break
            period_timestamps.append(current_td_sec)
            current_td_sec += timedelta(seconds=1)
        logs = self.logs()
        logs_periods = [[] for i in range(len(period_timestamps)-1)]
        period = 1
        index = 0
        while index < len(logs):
            if (not logs[index].is_timestamp_valid()) or (logs[index].timestamp <= period_timestamps[period]):
                (logs_periods[period-1]).append(logs[index])
                index +=1
            else:
                period +=1
        return period_timestamps, logs_periods