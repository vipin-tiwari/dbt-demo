# Databricks notebook source
# MAGIC %sh
# MAGIC 
# MAGIC nslookup statlas.prod.atl-paas.net

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC curl https://ne-dmz-atlassian.net.atlassian.com/statlas.prod.atl-paas.net/plato.dev.configs/experimental/manifest.zip

# COMMAND ----------



# COMMAND ----------

token = dbutils.secrets.get('dbt-projects-test', 'DB_DEV_CONSUMER_US_TOKEN')
print(len(token))
print(token)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC databricks secrets list --scope=dbt-projects-test

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install datadog
# MAGIC pip install zipfile36
# MAGIC pip install pyyaml
# MAGIC pip install pytest-shutil
# MAGIC pip install dbt-databricks
# MAGIC #dbt init dq_test

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install datadog

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC nslookup dp-plato-dmz-dev-us-east-1-statsd-atl-inf-net.net.atlassian.com

# COMMAND ----------


from datadog import DogStatsd
import yaml
result = {}
result['failures'] = 200

tags = ["env:dev", "region:us-east-1", "entity:hack-consumer", "quality_dimension:hack"]

# SIGNALFX_STATSD_HOST = '127.0.0.1'

SIGNALFX_STATSD_HOST = "dp-plato-dmz-dev-us-east-1-statsd-atl-inf-net.net.atlassian.comzzz"

signalfx_statsd_conn = DogStatsd(host=SIGNALFX_STATSD_HOST)

print(signalfx_statsd_conn)
print(result["failures"]/10)

status = signalfx_statsd_conn.gauge("vipin.metrics.consumer", result["failures"]/10, tags=tags)

print(status)

# COMMAND ----------


from datadog import DogStatsd
result = {}
result['failures'] = 300

tags = ["env:dev", "region:us-east-1", "entity:hack-consumer", "quality_dimension:hack"]

# SIGNALFX_STATSD_HOST = '127.0.0.1'

SIGNALFX_STATSD_HOST = "dp-plato-dmz-dev-us-east-1-statsd-atl-inf-net.net.atlassian.com"

signalfx_statsd_conn = DogStatsd(host=SIGNALFX_STATSD_HOST)

print(signalfx_statsd_conn)
print(result["failures"]/10)

signalfx_statsd_conn.gauge("plato.metrics.quality.checks", result["failures"]/10, tags=tags)

# COMMAND ----------

from databricks import sql

with sql.connect(server_hostname="atlassian-plato-dev-consumer-us-01.cloud.databricks.com",
                 http_path="/sql/1.0/endpoints/2e0ffc6b44006fe5",
                 access_token="dapi7c3d344a5f61ee8ddcf59a541e44e6cc") as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM jira_us.project_current LIMIT 2")
        result = cursor.fetchall()

        for row in result:
          print(row)

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install datadog
# MAGIC pip install zipfile36
# MAGIC pip install pyyaml
# MAGIC pip install pytest-shutil
# MAGIC pip install dbt-databricks
# MAGIC #dbt init dq_test

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC cat /dbfs/scripts/monitoring/conf/config.toml

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/FileStore/dbt-projects/aac_batch/

# COMMAND ----------

import zipfile
import os
with zipfile.ZipFile("/dbfs/FileStore/dbt-projects/aac_batch/aac_batch.zip","r") as zip_ref:
    zip_ref.extractall("/databricks/driver/dbt-projects/")
    print(os.listdir("/databricks/driver/dbt-projects/"))

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /databricks/driver/dbt-projects/version=001
# MAGIC 
# MAGIC echo "======="
# MAGIC 
# MAGIC ls /databricks/driver/dbt-projects/

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /databricks/driver/dbt-projects/version=001
# MAGIC pwd; ls ; 
# MAGIC 
# MAGIC echo "=============="
# MAGIC 
# MAGIC cp /dbfs/FileStore/dbt-projects/aac_batch/profiles.yml /databricks/driver/dbt-projects/
# MAGIC 
# MAGIC ls /databricks/driver/dbt-projects/;
# MAGIC 
# MAGIC cd /databricks/driver/dbt-projects/aac_batch;
# MAGIC 
# MAGIC ls ../
# MAGIC 
# MAGIC export DB_DEV_CONSUMER_US_TOKEN="dapi7c3d344a5f61ee8ddcf59a541e44e6cc"
# MAGIC export BITBUCKET_BRANCH="master"
# MAGIC dbt run --profiles-dir=../ --target=dev-producer-us-jira-us

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/FileStore/dbt-projects/aac_batch/profiles.yml /databricks/driver/dbt-projects/
# MAGIC 
# MAGIC cd /databricks/driver/dbt-projects/version=001
# MAGIC 
# MAGIC pwd
# MAGIC 
# MAGIC dbt deps
# MAGIC 
# MAGIC export DB_DEV_CONSUMER_US_TOKEN="dapi7c3d344a5f61ee8ddcf59a541e44e6cc"
# MAGIC export BITBUCKET_BRANCH="master"
# MAGIC export BRANCH_NAME="master"
# MAGIC 
# MAGIC dbt test --profiles-dir=../ --target=dev-consumer-us-jira-us --select tag:reliability_test_daily

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC pwd
# MAGIC 
# MAGIC cat /databricks/driver/dbt-projects/version=001/target/run_results.json

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC ls /databricks/driver/dbt-projects/aac_batch/models/jira

# COMMAND ----------

import zipfile
import shutil
import os
import yaml
import sys
import subprocess
from io import open

class DbtAction:
    def __init__( self ):
        self.DBT_PROJECT_LOCATION = '/databricks/driver/dbt-projects/'

    def switch_working_directory(self, version):
        os.chdir(self.DBT_PROJECT_LOCATION + "version=001")

    def install_dependencies(self):
        cmd = Command("dbt deps;")
        cmd.execute()


    def test_command(self, profile, tag_selection_clause):
        base_command = """
          dbt test  --profiles-dir=../ --target=""" + profile + """ --select=""" + tag_selection_clause
        return Command(base_command)

    def run_command(self, profile, variables, selection_clause):
        base_command = """
            dbt run --profiles-dir=../  --vars=""" + variables + """ --target=""" + profile

        if selection_clause != 'all':
            return Command(base_command + """ --select=""" + selection_clause)
        else:
            return Command(base_command)

class Command():
    def __init__( self, cmd ):
        self.cmd = cmd
    def print_cmd(self):
        print(self.cmd)
    def execute( self ):
        final = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, shell=True)
        stdout, nothing = final.communicate()
        print("Command Run stdout: "+str(stdout))
        print("Command Run nothing: "+str(nothing))
        log = open('log', 'wb')
        log.write(stdout)
        log.close()
        final.terminate()

class Setup:
    def __init__( self ):
        self.FILESTORE_PATH =  '/dbfs/FileStore/'
        self.DBT_PROJECT_LOCATION = 'dbt-projects/'

    def extract_project_directory(self, project):
        print("Extracting project: ----> "+self.FILESTORE_PATH + self.DBT_PROJECT_LOCATION + project + '/' + project + '.zip')
        with zipfile.ZipFile(self.FILESTORE_PATH + self.DBT_PROJECT_LOCATION + project + '/' + project + '.zip', 'r') as zip_ref:
            zip_ref.extractall("/databricks/driver/"+self.DBT_PROJECT_LOCATION)
        print(os.listdir("/databricks/driver/"+self.DBT_PROJECT_LOCATION))

    def copy_profiles(self, project, profiles):
        print("Copying file : --->> "+'/dbfs/FileStore/dbt-projects/' + project + '/profiles.yml', '/databricks/driver/dbt-projects/'+profiles)
        shutil.copy('/dbfs/FileStore/dbt-projects/' + project + '/profiles.yml', '/databricks/driver/dbt-projects/'+profiles)

    def lookup_profile_token(self, profile):
        print("looking up token: --> "+profile)
        with open('/databricks/driver/dbt-projects/profiles.yml') as file:
            data_loaded = yaml.safe_load(file)
        profiles = data_loaded['version=001']['outputs']
        token_string = profiles[profile]['token']
        token_name = token_string.replace("{{ env_var('",'').replace("') }}",'')
        return token_name


# env, region, source directory, schedule_time--> db_job's
FILESTORE_PATH =  '/dbfs/FileStore/'
DBT_PROJECT_LOCATION = 'dbt-projects/'

project = "aac_batch"
profile = "dev-consumer-us-jira-us"
version = "version=001"
branch_name = "non-master"
variables = "'{\"namespace\":\"jira_us\"}'"
selection_clause = "tag:reliability_test_daily"
dbt_action_arg = "test"

dbt_setup = Setup()
dbt_action = DbtAction()

def initialize(project, profile_file):
    print("Triggering Project extract: "+project+" for profiles defined in : "+profile_file)
    dbt_setup.extract_project_directory(project)
    dbt_setup.copy_profiles(project, profile_file)

def execute(project, profile, branch_name, variables, selection_clause):
    env = os.environ
    token_name = dbt_setup.lookup_profile_token(profile)
    env[token_name] = dbutils.secrets.get('dbt-projects-test',token_name)
    env['BRANCH_NAME'] = branch_name
    env['BITBUCKET_BRANCH'] = branch_name

    dbt_action.switch_working_directory(project)
    dbt_action.install_dependencies()

    if dbt_action_arg == "run":
        print("Execute dbt run command....")
        cmd = dbt_action.run_command(profile, variables, selection_clause)
    elif dbt_action_arg == "test":
        print("Execute dbt test command....")
        cmd = dbt_action.test_command(profile, selection_clause)

    cmd.execute()


initialize(project, "profiles.yml")
execute(project, profile, branch_name, variables, selection_clause)

# COMMAND ----------

import sys
import os
import yaml
import json
from io import open
from datadog import DogStatsd

class DreChecks:
    def __init__( self ):
            self.metrics_client = Metrics()
    
    def collect_all_models(self, version, project):
        models_list = []

        for root, dirs, files in os.walk(version+'/models/'+project+"/jira"):
            for filename in files:
                if filename.lower().endswith('.yml'):
                    with open(os.path.join(root, filename), "r") as stream:
                        try:
                            model_name = yaml.safe_load(stream)["models"][0]["name"]
                            models_list.append(model_name)
                        except yaml.YAMLError as exc:
                            print(exc)

        return models_list



    def generate_metrics(self, models_list, version, env="dev", region="us-east-1"):
        with open(version+'/target/run_results.json', 'r') as f:
            data = json.load(f)

            for model_name in models_list:

                for result in data['results']:
                    if result['status'] == 'fail':
                        error_msg = result['message']
                        test_name = result['unique_id'].split(".")[2]

                        if model_name in test_name:
                            tmp_test_unique_id = test_name.replace(model_name, "DQ_Table")
                            test_name = tmp_test_unique_id.split("_DQ_Table_")[0]
                            column_name_list = tmp_test_unique_id.split("_DQ_Table_")[1].split("__")

                            metrics_tags = self.metrics_client.build_metrics_tags(env, region, model_name, test_name)
                            self.metrics_client.send_metrics("plato.metrics.quality.checks", result["failures"], metrics_tags)
                    else:
                        metrics_tags = self.metrics_client.build_metrics_tags(env, region, model_name, "")
                        self.metrics_client.send_metrics("plato.metrics.quality.checks", 0, metrics_tags)

class Metrics():
    def __init__( self ):
        self.SIGNALFX_STATSD_HOST = '127.0.0.1'
        self.signalfx_statsd_conn = DogStatsd(host=self.SIGNALFX_STATSD_HOST)

    def build_metrics_tags(self, env, region, model_name, test_name):
        tags = ["env:"+env, "region:"+region, "entity:"+model_name, "quality_dimension:"+test_name]

        return tags

    def send_metrics(self, metrics_name, metrics_value, metrics_tags):
        self.signalfx_statsd_conn.gauge(metrics_name, metrics_value, tags=metrics_tags)


# env, region, source directory, schedule_time--> db_job's
FILESTORE_PATH =  '/dbfs/FileStore/'
DBT_PROJECT_LOCATION = 'dbt-projects/'

project = "aac_batch"
version = "version=001"
env = "dev"
region = "us-east-1"

dre_checks = DreChecks()

os.chdir("/databricks/driver/"+DBT_PROJECT_LOCATION)

models_list = dre_checks.collect_all_models(version, project)
dre_checks.generate_metrics(models_list, version)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC pip install pyyaml

# COMMAND ----------

import argparse
import os
import subprocess

from src.dre.db_job import DbJobSetup
from src.dre.utils.config import DBFS_PATH_PREFIX


class DbtAction:
    def __init__(
        self,
        project,
        product,
        profile,
        version,
        variables,
        dbt_command,
        selection_clause,
    ):
        self.project = project
        self.product = product
        self.profile = profile
        self.version = version
        self.variables = variables
        self.dbt_command = dbt_command
        self.selection_clause = selection_clause

    def install_dependencies(self):
        os.chdir(DBFS_PATH_PREFIX + self.version)
        self.execute_cmd("dbt deps;")

    def build_base_arguments(self):
        return f"--profiles-dir=../ --vars={self.variables} --target={self.profile}"

    def build_models_clauses(self):
        return f" --models {self.project}.{self.product}"

    def build_dbt_test_command(self):
        print(f"Building Command for Dbt {self.dbt_command} ")
        dbt_command_args = self.build_base_arguments() + self.build_models_clauses()

        print(
            f"Final Dbt Command :---> dbt {self.dbt_command} {dbt_command_args} --select {self.selection_clause}"
        )
        return self.execute_cmd(
            f"dbt {self.dbt_command} {dbt_command_args} --select {self.selection_clause}"
        )

    @staticmethod
    def execute_cmd(cmd):
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            print("Command ran with error: " + str(stderr))
        else:
            print("Command ran: " + str(stdout))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--project", type=str, help="batch, realtime", required=True)
    parser.add_argument("--product", help="jira, opsgenie ", required=True)
    parser.add_argument("--profile", help="consumer profile", required=True)
    parser.add_argument("--version", help="module version", required=True)
    parser.add_argument("--branch", help="branch", required=True)
    parser.add_argument("--variables", help="dbt variables", required=True)
    parser.add_argument("--selection_clause", help="selection_clause", required=True)
    parser.add_argument(
        "--dbt_action_arg", help="action to run", required=True, choices=["test", "run"]
    )

    args = parser.parse_args()

    dbfs_job_setup = DbJobSetup(args.project, args.profile, args.version)

    dbfs_job_setup.copy_profiles()
    dbfs_job_setup.find_token()

    dbt_action = DbtAction(
        args.project,
        args.product,
        args.profile,
        args.version,
        args.variables,
        args.dbt_command,
        args.selection_clause,
    )
    dbt_action.install_dependencies()
    dbt_action.build_dbt_test_command()

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/FileStore/
# MAGIC 
# MAGIC pwd

# COMMAND ----------

import zipfile
import os

DBFS_PATH_PREFIX = "/dbfs/FileStore/dbt-projects/"


def extract_project_directory(self):
    if os.path.exists(f"{DBFS_PATH_PREFIX}{self.project}/{self.project}.zip"):
        print(f"Extracting project: ----> {DBFS_PATH_PREFIX}{self.project}/{self.project}.zip")

        with zipfile.ZipFile(f"{DBFS_PATH_PREFIX}{self.project}/{self.project}.zip", "r") as zip_ref:
            zip_ref.extractall(DBFS_PATH_PREFIX)
    else:
        raise FileNotFoundError(f"{DBFS_PATH_PREFIX}{self.project}/{self.project}.zip does not exist")



# COMMAND ----------

import sys
import os

pythonpath = os.getenv('PYTHONPATH')
print(pythonpath+":/dbfs/FileStore/dbt-projects/")

sys.path.append('/dbfs/FileStore/dbt-projects/')

os.environ['PYTHONPATH'] = pythonpath+":/dbfs/FileStore/dbt-projects/"

print(f"Printing SysPath :----> {sys.path}\n\n")
print(f"Printing PYTHONPATH :----> {os.getenv('PYTHONPATH')}\n\n")

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC echo $PYTHONPATH
# MAGIC 
# MAGIC # cd plugins/component/plato/

# COMMAND ----------


