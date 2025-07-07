import os
from datetime import datetime

import boto3
import sagemaker
from dotenv import load_dotenv
from sagemaker.workflow.function_step import step
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import LocalPipelineSession

# SageMaker Studio 環境
try:
    ROLE = sagemaker.get_execution_role()
    ACCOUNT_ID = SAGEMAKER_SESSION.account_id()
    SAGEMAKER_SESSION = sagemaker.session.Session()
    REGION = SAGEMAKER_SESSION._region_name


# SageMaker Studioの外部で実行する場合、環境変数を使用
except ValueError:
    load_dotenv()
    ROLE = os.getenv("SAGEMAKER_EXECUTION_ROLE")
    ACCOUNT_ID = os.getenv("SAGEMAKER_ACCOUNT_ID")
    SAGEMAKER_SESSION = sagemaker.session.Session()
    REGION = SAGEMAKER_SESSION._region_name

BUCKET = os.getenv("SAGEMAKER_BUCKET")
DATA_DIRECTORY = os.getenv("DATA_DIRECTORY")

print(f"Role: {ROLE}")
print(f"Account ID: {ACCOUNT_ID}")
print(f"Region: {REGION}")
print(f"Bucket: {BUCKET}")

# CPUイメージ
IMAGE_URI = (
    f"763104351884.dkr.ecr.{REGION}.amazonaws.com/pytorch-training:2.3-cpu-py311"
)


@step(
    name="test_step_func",
    keep_alive_period_in_seconds=30,
    image_uri=IMAGE_URI,
    instance_type="ml.m5.xlarge",  # instance type for the pipeline
    instance_count=1,
    dependencies="requirements.txt",  # dependencies for the pipeline
    role=ROLE,
)
def upload_text_file_to_s3(file_path: str):
    sample_test = "This is a test step in the pipeline."
    with open(file_path, "w") as f:
        f.write(sample_test)
    # upload the file to S3
    file_name = os.path.basename(file_path)
    key = f"{DATA_DIRECTORY}/{file_name}"
    s3client = boto3.client("s3")
    s3client.upload_file(
        Filename=f"{file_name}",
        Bucket=BUCKET,
        Key=key,
    )
    return f"s3://{BUCKET}/{key}"


def define_pipeline():
    input_file_param = ParameterString(
        name="input_file_param",
    )
    pipeline_name = "simplePipeline"
    pipeline = Pipeline(
        name=pipeline_name,
        steps=[upload_text_file_to_s3(input_file_param)],
        parameters=[input_file_param],
    )
    return pipeline


def define_local_pipeline():
    """Define the local mode pipeline."""
    input_file_param = ParameterString(
        name="input_file_param",
    )
    local_pipeline_session = LocalPipelineSession()
    pipeline_name = "localPipeline"
    local_pipeline = Pipeline(
        name=pipeline_name,
        steps=[upload_text_file_to_s3(input_file_param)],
        parameters=[input_file_param],
        sagemaker_session=local_pipeline_session,
    )
    return local_pipeline


def main():
    filename = f"sample_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    pipeline = define_pipeline()
    pipeline.upsert(role_arn=ROLE)
    execution = pipeline.start(
        parameters={
            "input_file_param": filename,
        }
    )
    execution.describe()
    execution.wait()
    final_data_location = execution.result(step_name="test_step_func")
    print(f"Final data location: {final_data_location}")


if __name__ == "__main__":
    main()
