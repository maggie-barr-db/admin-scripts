import requests
import json
from pyspark.sql import functions as f
from pyspark.sql.types import StructField, ArrayType, StructType, StringType
from pyspark.sql import Row
from pyspark.sql import SparkSession
from datetime import datetime   
import pytz

spark = SparkSession.builder.getOrCreate()

# get the runs
# TODO: Loop on the pages and add each list of run_ids to a larger list
databricks_url = "https://e2-demo-field-eng.cloud.databricks.com"
job_list_endpoint = "/api/2.1/jobs/runs/list"
token = "dapi0915bb5eb06f66ec8f3fcf4a75c20a7a-2"

start = "2025-09-23 08:00:00"
end = "2025-09-23 10:59:59"

def est_to_epoch_mil(datetime_str):
  local = pytz.timezone("US/Eastern")
  naive = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
  local_dt = local.localize(naive, is_dst=None)
  utc_dt = local_dt.astimezone(pytz.utc)
  return utc_dt.timestamp()

converted_start = est_to_epoch_mil(start)
start_time_from = str(int(converted_start * 1000))

converted_end = est_to_epoch_mil(end)
start_time_to = str(int(converted_end * 1000))

print("start_time_from: " + start_time_from)
print("start_time_to: " + start_time_to)


#####################################################################
#####################################################################
# GET ALL RUN IDS WHERE THE RUN FAILED
#####################################################################
#####################################################################

###################### STEP 1 ######################
# INITIAL RUN TO GET THE FIRST NEXT_PAGE_TOKEN

header = {'Authorization':'Bearer {}'.format(token), 'start_time_from':'{}'.format(start_time_from), 'start_time_to':'{}'.format(start_time_to)}
# payload = """{"page_token": "CAEQ-vidjJ8yIIPS-aqb_Hw="}"""
payload = """"""

resp = requests.get(
  databricks_url + job_list_endpoint,
  data=payload,
  headers=header
)

full_run_list = []

di = json.loads(resp.text)
next_page_token = di['next_page_token']
print(next_page_token)

for run in di['runs']:
  if run['state']['result_state'] == 'FAILED':

    output_runs = run['run_id']# [item.get('run_id') for item in di.get('runs', [])]
    full_run_list.append(output_runs)

###################### STEP 2 ######################
# PAGINATE TO GET THE REST OF THE RUNS

while di['has_more'] is True:
  
  header = {'Authorization':'Bearer {}'.format(token), 'start_time_from':'{}'.format(start_time_from), 'start_time_to':'{}'.format(start_time_to)}
  # payload = """{"page_token": "CAEQ-vidjJ8yIIPS-aqb_Hw="}"""
  payload = """{"page_token": "%s"}"""%(next_page_token)

  resp = requests.get(
    databricks_url + job_list_endpoint,
    data=payload,
    headers=header
  )

  di = json.loads(resp.text)
  next_page_token = di['next_page_token']

  for run in di['runs']:
    if run['state']['result_state'] == 'FAILED':

      output_runs = run['run_id']# [item.get('run_id') for item in di.get('runs', [])]
      full_run_list.append(output_runs)

#get the output of a job run

# databricks_url = "https://adb-4731905620902766.6.azuredatabricks.net"
job_list_endpoint = "/api/2.1/jobs/runs/get-output"

# token = "dapi0915bb5eb06f66ec8f3fcf4a75c20a7a-2"

# Initiate empty full_run_output_df
# columns = ["run_id", "run_name", "run_page_url", "state", "start_time", "end_time", "term_code", "error"]
my_schema = StructType([
    StructField("run_id", StringType()),
    StructField("run_name", StringType()),
    StructField("run_page_url", StringType()),
    StructField("state", StringType()),
    StructField("start_time", StringType()),
    StructField("end_time", StringType()),
    StructField("term_code", StringType()),
    StructField("error", StringType())
])
full_run_output_df = spark.createDataFrame([], schema=my_schema)

count = 0
for run_id in full_run_list:
  header = {'Authorization':'Bearer {}'.format(token)}
  payload = """{"run_id": "%s"}"""%(run_id)
  # job_list_endpoint = "api/2.0/jobs/runs/get-output?run_id=%s"%(run_id)

  resp = requests.get(
    databricks_url + job_list_endpoint,
    data=payload,
    headers=header
  )

  run_output = json.loads(resp.text)
  count = count+1
  print("trying count num: " + str(count) + " run_id number: " + str(run_id))

  try:
    run_name = run_output['metadata']['run_name']
    run_page_url = run_output['metadata']['run_page_url']
    state = run_output['metadata']['status']['state']
    start_time = run_output['metadata']['start_time']
    end_time = run_output['metadata']['end_time']
    term_code = run_output['metadata']['status']['termination_details']['code']
    error = run_output['error']

    data = [(run_id, run_name, run_page_url, state, start_time, end_time, term_code, error)]                                                                     

    run_output_row_df = spark.createDataFrame(data=data, schema=my_schema)
    full_run_output_df = full_run_output_df.union(run_output_row_df)
  except:
    data = [(run_id, "", "", "", "", "", "", "Could not fetch output for this run")]                                                                     

    run_output_row_df = spark.createDataFrame(data=data, schema=my_schema)
    full_run_output_df = full_run_output_df.union(run_output_row_df)


# full_run_output_df.display()
# full_run_output_df.write.mode('overwrite').saveAsTable("gps_dev.uc_analysis.job_output_analysis")