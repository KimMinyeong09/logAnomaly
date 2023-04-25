import logging
import sys
import os
import subprocess
import time
import json
import pickle

from drain3.template_miner_config import TemplateMinerConfig
from drain3 import TemplateMiner

# classify by block ID
# not yet...

logger = logging.getLogger(__name__)
# logging.basicConfig(filename="example.log", filemode="w", level=logging.INFO)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

in_gz_file = "HDFS_2k.tar.gz"
in_log_file = "example.log"

if not os.path.isfile(in_log_file):
    logger.info(f"Downloading file {in_gz_file}")
    p = subprocess.Popen(f"curl https://zenodo.org/record/3227177/files/{in_gz_file} --output {in_gz_file}", shell=True)
    p.wait()
    logger.info(f"Extracting file {in_gz_file}")
    p = subprocess.Popen(f"tar -xvzf {in_gz_file}", shell=True)
    p.wait()
    
config = TemplateMinerConfig()
config.load(os.path.dirname(__file__) + "/drain3.ini")
config.profiling_enabled = True
template_miner = TemplateMiner(config=config)

print()

line_count = 0

with open(in_log_file) as f:
    lines = f.readlines()

start_time = time.time()
batch_start_time = start_time
batch_size = 10000

for line in lines:
    line = line.rstrip()
    line = line.partition(": ")[2]
    # print(line)
    result = template_miner.add_log_message(line)
    para = template_miner.extract_parameters
    # print(result)
    line_count += 1
    if line_count % batch_size == 0:
        time_took = time.time() - batch_start_time
        rate = batch_size / time_took
        logger.info(f"Processing line: {line_count}, rate {rate:.1f} lines/sec, "
                    f"{len(template_miner.drain.clusters)} clusters so far.")
        batch_start_time = time.time()
    if result["change_type"] != "none":
        result_json = json.dumps(result)
        logger.info(f"Input ({line_count}) >> " + line)
        logger.info("Result: " + result_json)

time_took = time.time() - start_time
rate = line_count / time_took
logger.info(f"\n--- Done processing file in {time_took:.2f} sec. Total of {line_count} lines, rate {rate:.1f} lines/sec, "
            f"{len(template_miner.drain.clusters)} clusters")

print()

print('--- Save template to pickle')


folder_path = "logEvent"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

with open("logEvent/logEventList.pickle", "wb") as f:
    pickle.dump(template_miner, f)

for i in template_miner.drain.clusters:
    print(f'ID{i.cluster_id}:: {i.get_template()}')


'''
with open("data.pickle", "rb") as f:
    template_miner = pickle.load(f)
    
for i in template_miner.drain.clusters:
    print(f'ID{i.cluster_id}:: {i.get_template()}')
'''