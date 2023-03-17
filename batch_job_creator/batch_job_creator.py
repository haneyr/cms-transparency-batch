#Copyright 2023 Google LLC

#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

import sys
from datetime import datetime
from google.cloud import storage
from google.cloud import batch_v1

def create_container_job(project_id: str, region: str, job_name: str, env_dict: dict, task_count: int) -> batch_v1.Job:

    client = batch_v1.BatchServiceClient()

    # Define what will be done as part of the job.
    runnable = batch_v1.Runnable()
    runnable.container = batch_v1.Runnable.Container()
    runnable.container.image_uri = "gcr.io/shared-analytics-services/batch-processor-ubuntu"

    task = batch_v1.TaskSpec()
    task.runnables = [runnable]
    #Pass environment variable dictionary to the task
    task.environment.variables = env_dict


    # We can specify what resources are requested by each task.
    resources = batch_v1.ComputeResource()
    resources.cpu_milli = 3000  # in milliseconds per cpu-second. This means the task requires 2 whole CPUs.
    resources.memory_mib = 4096  # in MiB
    task.compute_resource = resources

    task.max_retry_count = 1
    task.max_run_duration = "14400s"

    # Tasks are grouped inside a job using TaskGroups.
    # Currently, it's possible to have only one task group.
    group = batch_v1.TaskGroup()
    group.parallelism = task_count
    group.task_count = task_count
    group.task_spec = task
    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = "e2-standard-8"
    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    network_if = batch_v1.AllocationPolicy.NetworkInterface()
    network_if.network = "projects/shared-network-370014/global/networks/prod-vpc-shared" #Change this to reflect your network
    network_if.subnetwork = "projects/shared-network-370014/regions/us-central1/subnetworks/prod-vpc-shared" #Change this to reflect your subnetwork
    network_if.no_external_ip_address = True #Set this to False if you're ok with public IP addresses
    network = batch_v1.AllocationPolicy.NetworkPolicy()
    network.network_interfaces = [network_if]
    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [instances]
    allocation_policy.network = network

    job = batch_v1.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.labels = {"env": "testing", "type": "container"}
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    create_request = batch_v1.CreateJobRequest()
    create_request.job = job
    create_request.job_id = job_name
    create_request.parent = f"projects/{project_id}/locations/{region}"

    return client.create_job(create_request)

def download_blob(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    contents = blob.download_as_string()
    return contents

def create_env_vars(contents):
    i = 0
    env_vars = {} 
    for line in contents.splitlines():
        url = line.decode()
        env_vars[f"URL{i}"] = url
        i += 1
    return env_vars, i

def main(argv):
    project_id = "shared-analytics-services"
    region = "us-central1"
    now = datetime.now()
    bucket_name = "cms-config-files" #Change this to reflect the bucket the index files (uhc_202303.config, etc) are located in
    blob_name = sys.argv[1]
    contents = download_blob(bucket_name,blob_name) 
    env_dict, task_count = create_env_vars(contents)
    job_name = "job"+now.strftime("%H-%M-%S-%f")
    create_container_job(project_id, region, job_name, env_dict, task_count)
    print(f"Batch job {job_name} created from {blob_name} file list")

if __name__ == "__main__":
   main(sys.argv[1:])
