# Amazon SageMaker Pipelines ハンズオンリポジトリ

このリポジトリは、ブログ記事 **[【インターン体験記】Amazon SageMaker Pipelines入門：基本からDataTroveとの連携まで](https://hack.nikkei.com/blog/intern_sagemaker_pipelines)** 
のハンズオン用サンプルコードです。

記事と合わせて本リポジトリのコードを実行することで、SageMaker Pipelinesの基本的な使い方と、Hugging Face製の大規模データ処理ライブラリDataTroveを組み合わせた、より実践的なデータ処理パイプラインの構築方法を学べます。SageMaker Studio環境とローカルPC環境の両方で動作するコード構成になっており、AWSリソースを使わずにローカルのDocker環境でパイプラインを実行する方法も含まれています。

## このリポジトリに含まれるもの
*   **基本的なパイプラインの作成**: 簡単なテキストファイルをS3にアップロードするパイプライン (`simple_sm_pipelines.py`)
*   **DataTroveを用いたパイプライン**: DataTroveを使ってJSONL形式のデータをフィルタリングする、より実践的なパイプライン (`datatrove_sm_pipelines.py`)
*   **簡単なDataTroveの使い方**: DataTroveを使った基本的なパイプライン（`simple_datatrove.py`）
*   **ローカルモードでの実行**: AWSリソースを使わずに、ローカルDocker環境でローカルモードのパイプラインを実行する方法

## 動作環境
*   Python 3.11

## セットアップ方法

### 1. リポジトリをクローン


```bash
git clone https://github.com/your-repository-link.git
cd how-to-create-simple-sagemaker-pipelines
```

### 2. パッケージのインストール

高速なパッケージ管理ツール `uv` の利用を推奨します。

```bash
# uv を使う場合
uv sync --frozen
```

`pip` を使う場合は、以下のコマンドを実行してください。

```bash
# pip を使う場合
pip install -r requirements.txt
```

### 3. AWS認証情報の設定

ローカルPCからパイプラインを実行するには、AWSの認証情報が必要です。プロジェクトのルートディレクトリに `.env` ファイルを新規作成し、以下の内容を記述してください。

`.env` ファイル
```
SAGEMAKER_EXECUTION_ROLE="arn:aws:iam::123456789012:role/service-role/AmazonSageMaker-ExecutionRole-123456789012"
SAGEMAKER_ACCOUNT_ID="123456789012"
SAGEMAKER_BUCKET="<your-s3-bucket-name>"
DATA_DIRECTORY="<your-directory-path>"
```

ご自身の環境に合わせて、以下の値を書き換えてください。

*   `SAGEMAKER_EXECUTION_ROLE`: SageMakerが各種AWSリソース（S3など）にアクセスするために使用するIAMロールのARN。
*   `SAGEMAKER_ACCOUNT_ID`: ご自身のAWSアカウントID。
*   `SAGEMAKER_BUCKET`: パイプラインの成果物（生成されたファイルなど）を保存するS3バケット名。
*   `DATA_DIRECTORY`: `SAGEMAKER_BUCKET`内で、成果物を保存するディレクトリ名。

## 実行方法

### 基本的なパイプライン

以下のコマンドで、S3にテキストファイルをアップロードする基本的なパイプラインを実行します。

```bash
uv run python simple_sm_pipelines.py
```

### Datatroveを使ったパイプライン

以下のコマンドで、DataTroveによるデータフィルタリングを行うパイプラインを実行します。

```bash
uv run python datatrove_sm_pipelines.py
```

実行が完了すると、`.env`で指定した`DATA_DIRECTORY`に、処理済みのファイル `filtered.jsonl` と、除外されたデータ `excluded.jsonl` が出力されます。

## ブログ記事

本コードの詳細な解説は、以下の記事をご覧ください。

**[【インターン体験記】Amazon SageMaker Pipelines入門：基本からDataTroveとの連携まで](https://hack.nikkei.com/blog/intern_sagemaker_pipelines)**
