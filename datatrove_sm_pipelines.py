import os

import boto3
import sagemaker
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.filters import LambdaFilter
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers import JsonlWriter
from dotenv import load_dotenv
from sagemaker.workflow.function_step import step
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.pipeline import Pipeline

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


def create_sample_jsonl_file():
    """サンプルjsonlファイルを作成し、S3にアップロードする"""
    filename = "sample.jsonl"
    sample_jsonl = """{"id": "1","title": "Test Document","text": "This is a test document."}
{"id": "2","title": "Another Document","text": "This document is also a test but has more text than the first one."}
{"id": "3","title": "Excluded Document","text": "Short."}
{"id": "4","title": "Long Document","text": "This document has enough text to be included in the output."}
"""
    with open(filename, "w") as f:
        f.write(sample_jsonl)
    key = os.path.join(DATA_DIRECTORY, filename)
    s3client = boto3.client("s3")
    # S3にファイルをアップロード
    print(f"Uploading {filename} to s3://{BUCKET}/{key}")
    s3client.upload_file(
        Filename=f"{filename}",
        Bucket=BUCKET,
        Key=key,
    )


@step(
    name="datatrove_step_func",
    keep_alive_period_in_seconds=300,
    image_uri=IMAGE_URI,
    instance_type="ml.m5.xlarge",  # パイプラインのインスタンスタイプ
    instance_count=1,
    dependencies="requirements.txt",  # パイプラインの依存関係
    role=ROLE,  # パイプラインの実行ロール
)
def filter_and_write_jsonl(input_file: str, output_dir: str) -> str:
    pipeline = [
        JsonlReader(data_folder=input_file, text_key="text"),
        # テキストの長さが10文字未満のものを除外する
        LambdaFilter(
            filter_function=lambda doc: len(doc.text) >= 10,
            # 除外されたデータも別に書き出せる
            exclusion_writer=JsonlWriter(
                output_dir,
                output_filename="excluded.jsonl",
                compression=None,
            ),
        ),
        JsonlWriter(output_dir, output_filename="filtered.jsonl", compression=None),
    ]

    executor = LocalPipelineExecutor(pipeline)
    executor.run()
    output_file = os.path.join(output_dir, "filtered.jsonl")
    return output_file


def define_pipeline():
    input_file_param = ParameterString(
        name="input_file_param",
    )
    output_dir_param = ParameterString(
        name="output_dir_param",
    )
    pipeline_name = "datatrovePipeline"  # パイプラインの名前
    pipeline = Pipeline(
        name=pipeline_name,
        steps=[filter_and_write_jsonl(input_file_param, output_dir_param)],
        parameters=[
            input_file_param,
            output_dir_param,
        ],
    )
    return pipeline


def main():
    create_sample_jsonl_file()
    # パイプラインを作成して開始
    pipeline = define_pipeline()
    pipeline.upsert(role_arn=ROLE)
    input_file = f"s3://{BUCKET}/{DATA_DIRECTORY}/sample.jsonl"
    output_dir = f"s3://{BUCKET}/{DATA_DIRECTORY}/"
    execution = pipeline.start(
        parameters={
            "input_file_param": input_file,
            "output_dir_param": output_dir,
        }
    )
    execution.describe()
    execution.wait()


if __name__ == "__main__":
    main()
