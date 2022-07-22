# Databricks notebook source
from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.dbgems import get_cloud, get_notebook_dir
import hashlib
import json
import re

class NotebookSolutionCompanion():
  """
  A class to provision companion assets for a notebook-based solution, includingn job, cluster(s), DLT pipeline(s) and DBSQL dashboard(s)
  """
  
  def __init__(self):
    self.solution_code_name = get_notebook_dir().split('/')[-1]
    self.cloud = get_cloud()
    self.solacc_path = get_notebook_dir()
    hash_code = hashlib.sha256(self.solacc_path.encode()).hexdigest()
    self.job_name = f"[SOLACC] {self.solution_code_name} | {hash_code}" # use hash to differentiate solutions deployed to different paths
    self.client = DBAcademyRestClient() # use dbacademy rest client for illustration. Feel free to update it to use other clients
    
  @staticmethod
  def convert_job_cluster_to_cluster(job_cluster_params):
    params = job_cluster_params["new_cluster"]
    params["cluster_name"] = f"""{job_cluster_params["job_cluster_key"]}"""
    params["autotermination_minutes"] = 45 # adding a default autotermination as best practice
    return params
  
  @staticmethod
  def create_or_update_job_by_name(client, params):
    """Look up the companion job by name and resets it with the given param and return job id; create a new job if a job with that name does not exist"""
    jobs = client.jobs().list()
    jobs_matched = list(filter(lambda job: job["settings"]["name"] == params["name"], jobs)) 
    assert len(jobs_matched) <= 1, f"""Two jobs with the same name {params["name"]} exist; please manually inspect them to make sure solacc job names are unique"""
    job_id = jobs_matched[0]["job_id"] if len(jobs_matched)  == 1 else None
    if job_id: 
      reset_params = {"job_id": job_id,
                     "new_settings": params}
      json_response = client.execute_post_json(f"{client.endpoint}/api/2.1/jobs/reset", reset_params) # returns {} if status is 200
      assert json_response == {}, "Job reset returned non-200 status"
      print(f"""Reset the {params["name"]} job with job_id {job_id} to original definition""")
    else:
      json_response = client.execute_post_json(f"{client.endpoint}/api/2.1/jobs/create", params)
      job_id = json_response["job_id"]
      print(f"""Created {params["name"]} job with job_id {job_id}""")
    return 
  
  # Note these functions assume that names for solacc jobs/cluster/pipelines are unique, which is guaranteed if solacc jobs/cluster/pipelines are created from this class only
  @staticmethod
  def create_or_update_pipeline_by_name(client, dlt_config_table, pipeline_name, dlt_definition_dict, spark):
    """Look up a companion pipeline by name and edit with the given param and return pipeline id; create a new pipeline if a pipeline with that name does not exist"""
    if not spark.catalog.tableExists(dlt_config_table):
      pipeline_id = None
    else:
      dlt_id_pdf = spark.table(dlt_config_table).filter(f"solacc = '{pipeline_name}'").toPandas()
      assert len(dlt_id_pdf) <= 1, f"two pipelines with the same name {pipeline_name} exist in the {dlt_config_table} table; please manually inspect the table to make sure pipelines names are unique"
      pipeline_id = dlt_id_pdf['pipeline_id'][0] if len(dlt_id_pdf) > 0 else None
      
    if pipeline_id:
        dlt_definition_dict['id'] = pipeline_id
        print(f"Found dlt {pipeline_name} at '{pipeline_id}'; updating it with latest config if there is any change")
        client.execute_put_json(f"{client.endpoint}/api/2.0/pipelines/{pipeline_id}", dlt_definition_dict)
    else:
        response = DBAcademyRestClient().pipelines().create_from_dict(dlt_definition_dict)
        pipeline_id = response["pipeline_id"]
        # log pipeline id to the cicd dlt table: we use this delta table to store pipeline id information because looking up pipeline id via API can sometimes bring back a lot of data into memory and cause OOM error; this table is user-specific
        # Reusing the DLT pipeline allows for DLT run history to accumulate over time rather than to be wiped out after each deployment. DLT has some UI components that only show up after the pipeline is executed at least twice. 
        spark.createDataFrame([{"solacc": pipeline_name, "pipeline_id": pipeline_id}]).write.mode("append").option("mergeSchema", "True").saveAsTable(dlt_config_table)
        
    return pipeline_id
  
  @staticmethod
  def create_or_update_cluster_by_name(client, params):
      """Look up a companion cluster by name and edit with the given param and return cluster id; create a new cluster if a cluster with that name does not exist"""
      clusters = client.execute_get_json(f"{client.endpoint}/api/2.0/clusters/list")["clusters"]
      clusters_matched = list(filter(lambda cluster: params["cluster_name"] == cluster["cluster_name"], clusters))
      cluster_id = clusters_matched[0]["cluster_id"] if len(clusters_matched) == 1 else None
      if cluster_id: 
        params["cluster_id"] = cluster_id
        json_response = client.execute_post_json(f"{client.endpoint}/api/2.0/clusters/edit", params) # returns {} if status is 200
        assert json_response == {}, "Job reset returned non-200 status"
        print(f"""Reset the {params["cluster_name"]} cluster with cluster_id {cluster_id} to original definition""")
      else:
        json_response = client.execute_post_json(f"{client.endpoint}/api/2.0/clusters/create", params)
        cluster_id = json_response["cluster_id"]
        print(f"""Created {params["cluster_name"]} cluster with cluster_id {cluster_id}""")
      return 
    
  @staticmethod
  def customize_job_json(input_json, job_name, solacc_path, cloud):
    input_json["name"] = job_name

    for i, _ in enumerate(input_json["tasks"]):
      notebook_name = input_json["tasks"][i]["notebook_task"]['notebook_path']
      input_json["tasks"][i]["notebook_task"]['notebook_path'] = solacc_path + "/" + notebook_name

    for j, _ in enumerate(input_json["job_clusters"]):
      node_type_id_dict = input_json["job_clusters"][j]["new_cluster"]["node_type_id"]
      input_json["job_clusters"][j]["new_cluster"]["node_type_id"] = node_type_id_dict[cloud]
    job_json = input_json
    if cloud == "AWS": 
      job_json["job_clusters"][0]["new_cluster"]["aws_attributes"] = {
                        "ebs_volume_count": 0,
                        "availability": "ON_DEMAND",
                        "first_on_demand": 1
                    }
    if cloud == "MSA": 
      job_json["job_clusters"][0]["new_cluster"]["azure_attributes"] = {
                        "availability": "ON_DEMAND_AZURE",
                        "first_on_demand": 1
                    }
    if cloud == "GCP": 
      job_json["job_clusters"][0]["new_cluster"]["gcp_attributes"] = {
                        "use_preemptible_executors": False
                    }
    return job_json
    
  def get_job_param_json(self, input_json):
    self.job_params = self.customize_job_json(input_json, self.job_name, self.solacc_path, self.cloud)
  
  def deploy_compute(self, input_json):
    self.get_job_param_json(input_json)
    self.create_or_update_job_by_name(self.client, self.job_params)
    for job_cluster_params in self.job_params["job_clusters"]:
      self.create_or_update_cluster_by_name(self.client, self.convert_job_cluster_to_cluster(job_cluster_params))
      
  def deploy_pipeline(self, input_json, spark):
    pipeline_name = input_json.pop("name")
    self.create_or_update_pipeline_by_name(self.client, dlt_config_table, pipeline_name, input_json, spark)
    
  def deploy_dbsql(self, input_json):
    pass

