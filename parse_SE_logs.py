# import yaml
from SearchEngine import *
from common_functions import *

# with open(log_checker_folder + "configurationSE.yml", 'r') as stream:
#     try:
#         conf = yaml.safe_load(stream)
#     except yaml.YAMLError as error:
#         exit_and_fail(f"Cannot read configurationSE.yml file\n\t{error}") 

# ns = conf['namespace']
# logplane = conf['analysis']['logplane']['name']
# logs_per_logplane = conf['analysis']['logplane']['logs']

log_checker_folder = "/local/zarlapm/git/sig_challengers/.logChecker/"
namespace = "5g-bsf-zarlapm"
logplane_size = 10000


def main():
    search_engine = SearchEngine(namespace)
    search_engine.store_logs(log_checker_folder, logplane_size)
    search_engine.fetch_logplanes_logs(log_checker_folder)
    search_engine.create_container_folders(log_checker_folder)
    search_engine.separate_logs(log_checker_folder)


if __name__ == "__main__":
    main()