from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.filters import LambdaFilter
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.writers import JsonlWriter

input_file = "sample.jsonl"
output_dir = "output"


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
