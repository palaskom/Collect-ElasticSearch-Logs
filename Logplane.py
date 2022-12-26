from common_functions import *
from Container import *


class Logplane:

    def __init__(self, name=None):
        self.name = name
        self.dates = []
        self.containers = []


    def add_date(self, date=None):
        if date != None:
            if date not in self.dates:
                self.dates.append(date)
            else:
                exit_and_fail(f"Logplanes cannot contain identical dates")


    def get_fullname(self, date=None) -> str:
        if date == None:
            return self.name + "-" + self.dates[0]
        else:
            for i in range(0, len(self.dates)):
                if date == self.dates[i]:
                    return self.name + "-" + self.dates[i]
            warning(f"There is no date '{date}' in '{self.name}' logplane")
            return None


    def get_folder(self, parent=None):
        if parent == None:
            return self.name + "/"
        else:
            return parent + "/" + self.name + "/"


    def log_field(self, field:str, fields_dict:dict):
        if field in fields_dict.keys():
            return fields_dict[field]
        else:
            exit_and_fail(f"There is no {field} field in logplane {self.name}")


    def fetch_logs(self, file:str):
        data = read_logs_from_file(file, reading_mode="obj")
        logs = data['hits']['hits']
        for log in logs:
            logs_source = log['_source']
            version = self.log_field("version", logs_source)
            severity = self.log_field("severity", logs_source)
            timestamp = self.log_field("timestamp", logs_source)
            message = self.log_field("message", logs_source)
            service_id = self.log_field("service_id", logs_source)
            metadata = logs_source['metadata']
            pod_name = self.log_field("pod_name", metadata)
            container_name = self.log_field("container_name", metadata)
            # Update containers
            container_exists = False
            for container in self.containers:
                if (container.service_id == service_id) and (container.name == container_name) and (container.pod_name == pod_name):
                    container_exists = True
                    container.add_log(version, timestamp, service_id, severity, message)
                    break
            if not container_exists:
                new_container = Container(service_id, pod_name, container_name)
                new_container.add_log(version, timestamp, service_id, severity, message)
                self.containers.append(new_container)


