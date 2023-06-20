import logging
from datetime import datetime
import azure.functions as func
import azureml
from azureml.core import Workspace, Experiment, Datastore
from azureml.pipeline.core import PipelineData, Pipeline
from azureml.data.data_reference import DataReference
from azureml.pipeline.steps import PythonScriptStep
import pandas as pd
from azureml.core.compute import ComputeTarget, ComputeInstance
from azureml.core.compute_target import ComputeTargetException
import os


def main(myblob: func.InputStream):
    # Set up Azure ML workspace and experiment
    workspace = Workspace.get(name="mlflowtask", subscription_id="3887aac9-9e3e-4aa3-8859-0f5ab8e98a6f", resource_group="mlops-demo")
    experiment = Experiment(workspace=workspace, name="taskmlflow")
    blob_output_datastore_name = "outputdatastore"
    blob_input_datastore_name = "inputdatastore"
    blob_input_datastore = Datastore.register_azure_blob_container(
           workspace=workspace,
           datastore_name=blob_input_datastore_name,
           account_name="blobstorageaccount8b04cc",
           container_name="demo-data",
           account_key="oOVa6CNOjw0G1v8plPyARdw/n3uhfKXsxWWmjPg8U3GwflDmDJFFaHMbW9iFDF6T8uVTeTTWinK3+AStg9MVCw==")
    blob_output_datastore = Datastore.register_azure_blob_container(
           workspace=workspace,
           datastore_name=blob_output_datastore_name,
           account_name="blobstrgdestinationacc",
           container_name="demo-data",
           account_key="v37Qf9R7d8p/sRwVnHG7xNKHxEVrvQANoy1/a7EXwiV0b5TipUdkoh5DX5mbpezY10v2IjhQucnV+AStgSRzSQ==")
    output_data = PipelineData("output_data", datastore=Datastore(workspace, blob_output_datastore_name))
    input_data_1 = DataReference(datastore=Datastore(workspace, blob_input_datastore_name),data_reference_name="departmentdata", 
                                        path_on_datastore="/department-data.csv")
    input_data_2 = DataReference(datastore=Datastore(workspace, blob_input_datastore_name),data_reference_name="employeedata", 
                                        path_on_datastore="/employee-data.csv")
    input_data_version = datetime.now().strftime("%Y%m%d%H%M%S")
    mlflow_env = azureml.core.Environment.from_conda_specification(name="mlflow-env", file_path="conda.yml")
    script_name = "validate_and_combine.py"
    script_params = [
        "--input1", input_data_1,
        "--input2", input_data_2,
        "--output", output_data,
    ]
    compute_name = "taskmlflow-instance"
    compute_config = ComputeInstance.provisioning_configuration(
        vm_size="Standard_DS2_v2"
    )
    try:
        compute_instance = ComputeTarget(workspace, compute_name)
        print("Found existing compute instance.")
    except ComputeTargetException:
        compute_instance = ComputeInstance.create(workspace, compute_name, compute_config)
        compute_instance.wait_for_completion(show_output=True)
    validation_combination_step = PythonScriptStep(
        name="Validation and Combination",
        source_directory = os.path.dirname(os.path.realpath(__file__)),
        script_name=script_name,
        arguments=script_params,
        inputs=[input_data_1, input_data_2],
        outputs=[output_data],
        compute_target="taskmlflow-instance",
        runconfig={
            "environment": mlflow_env
        }
    )
    pipeline = Pipeline(workspace=workspace, steps=[validation_combination_step])
    print(pipeline)
    pipeline.validate()
    pipeline_run = experiment.submit(pipeline)
    print("pipeline Waiting")
    pipeline_run.wait_for_completion()
    logging.info("MLflow pipeline triggered successfully")
