from Logplane import *
from common_functions import *

class SearchEngine:

    def __init__(self, namespace=None) -> None:
        if namespace == None:
            warning("Every Search Engine object is instantiated for a specific namespace")
            exit_and_fail("No namespace provided")
        self.name = "search-engine"
        self.namespace = namespace
        self.logplanes = []

        output = run_cmd(os.getcwd(), self.es_rest(self.namespace, "GET", "/_cat/indices?v&h=index"))
        output_splitted = str(output).split("'")[1].split('\\n')
        logplane_fullnames = output_splitted[1:len(output_splitted)-1]
        for logplane_fullname in logplane_fullnames:
            if "adp" in logplane_fullname:
                continue 
            logplane_name, logplane_date = self.split_logplane_fullname(logplane_fullname)
            logplane_name_exists = False
            for logplane in self.logplanes:
                if logplane_name == logplane.name:
                    logplane_name_exists = True
                    logplane.add_date(logplane_date)
                    break
            if not logplane_name_exists:
                new_logplane = Logplane(logplane_name)
                new_logplane.add_date(logplane_date)
                self.logplanes.append(new_logplane)


    def es_rest(self, namespace:str, action:str, target:str) -> str:
        k = f"kubectl --namespace {namespace}"
        es_rest = f"{k} exec -c ingest $({k} get pods " + "| grep \"engine-ingest-tls\" | awk '{print $1}') -- /bin/esRest"
        return f"{es_rest} {action} \"{target}\" " # leave a space at the end


    def split_logplane_fullname(self, fullname):
        fullname_splitted = fullname.split('-')
        splitted_parts = len(fullname_splitted)
        date = fullname_splitted[splitted_parts-1]
        name = ""
        for i in range(0, splitted_parts-1):
            name += fullname_splitted[i]+'-'
        name = name[:len(name)-1]
        return name, date


    def get_folder(self, parent=None):
        if parent == None:
            return self.name + "/"
        return parent + "/" + self.name + "/"


    def store_logs(self, logs_folder, logplane_size) -> None:
        se_folder = self.get_folder(logs_folder)
        if os.path.exists(se_folder):
            run_cmd(logs_folder, f"rm -rf {self.name}")
        run_cmd(logs_folder, f"mkdir {self.name}")
        for logplane in self.logplanes:
            run_cmd(se_folder, f"mkdir {logplane.name}")
            logplane_folder = logplane.get_folder(parent=se_folder)
            for logplane_date in logplane.dates:
                logplane_fullname = logplane.get_fullname(logplane_date)
                es_rest = self.es_rest(self.namespace, "GET", f"/{logplane_fullname}/_search?size={logplane_size}")
                run_cmd(logplane_folder, es_rest + f"| jq > logs-{logplane_date}.json")
        info("Logs from all logplanes have been stored in files")
        return se_folder


    def fetch_logplanes_logs(self, logs_folder):
        se_folder = self.get_folder(logs_folder)
        for logplane in self.logplanes:
            for logplane_date in logplane.dates:
                logplane.fetch_logs(logplane.get_folder(se_folder) + f"logs-{logplane_date}.json")
                info(f"Logs from '{logplane.get_fullname(logplane_date)}' logplane have been fetched")

    
    def create_container_folders(self, logs_folder):
        se_folder = self.get_folder(logs_folder)
        for logplane in self.logplanes:
            logplane_folder = logplane.get_folder(se_folder)
            for container in logplane.containers:
                container.create_folder(logplane_folder)


    def separate_logs(self, logs_folder):
        se_folder = self.get_folder(logs_folder)
        for logplane in self.logplanes:
            logplane_folder = logplane.get_folder(se_folder)
            for container in logplane.containers:
                container_folder = container.get_folder(logplane_folder)
                validation_folder_name = "validation-analysis"
                create_folder(container_folder, validation_folder_name)
                validation_folder = container_folder + validation_folder_name + "/"
                write_logs_to_file(container_folder, "all-logs.json", container.logs)
                valid_logs, multiline_logs, no_json_logs, invalid_fields_logs = container.separate_SE_logs()
                write_logs_to_file(validation_folder, "valid-logs.txt", valid_logs)
                write_logs_to_file(validation_folder, "invalid-logs-multiline.txt", multiline_logs)
                write_logs_to_file(validation_folder, "invalid-logs-no_json_format.txt", no_json_logs)
                write_logs_to_file(validation_folder, "invalid-logs-invalid_mandatory_fields.txt", invalid_fields_logs)