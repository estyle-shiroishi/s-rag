import os
import logging
import base64
import json
import traceback
import io
import csv
from typing import List

import azure.functions as func
from azure.storage.queue import QueueClient
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError

from utils.blobs.blob_manager import BlobManager
from utils.mapping.chunk_mapping_manager import ChunkMappingManager
from utils.searchers.keyword_search import KeywordSearch
from utils.searchers.vector_search import VectorSearch
from utils.blob_document_processor import BlobDocumentProcessor
from utils.azure_embedder import AzureEmbedder
from utils.document_parser import DocumentParser

from config import (
    BLOB_STORAGE_URI,
    QUEUE_STORAGE_URI,
    QUEUE_NAME,
    DOC_CONTAINER_NAME,
    DB_CONTAINER_NAME,
    DI_API_ENDPOINT,
    DI_API_KEY,
    AOAI_API_KEY,
    AOAI_API_ENDPOINT,
    EMBEDDING_DEPLOYMENT_NAME,
    EMBEDDING_VERSION
)


from genie_bp import genie_bp

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

app.register_blueprint(genie_bp)

blob_manager = BlobManager(account_url=BLOB_STORAGE_URI, credential=DefaultAzureCredential())

parser = DocumentParser(DI_API_ENDPOINT, DI_API_KEY)

embedder = AzureEmbedder(
            api_key=AOAI_API_KEY,
            api_version=EMBEDDING_VERSION,
            azure_endpoint=AOAI_API_ENDPOINT,
            deployment_name=EMBEDDING_DEPLOYMENT_NAME
        )

mapping_manager = ChunkMappingManager(blob_manager, searchers = {
                'keyword': KeywordSearch(blob_manager, DB_CONTAINER_NAME, 'indexes/keyword_index'),
                'vector': VectorSearch(blob_manager, embedder, DB_CONTAINER_NAME, 'indexes/vector_index')
            })

@app.blob_trigger(arg_name="myblob", path=f"{DOC_CONTAINER_NAME}/{{name}}",
                               connection="BlobStorageConnection") 
def blob_trigger(myblob: func.InputStream):
    """
    Blob トリガー関数です。Blobにファイルがアップロードされた際に実行されます。
    追加されたファイルをインデックス化するためのキューに追加します。

    Parameters:
        myblob (func.InputStream): トリガーとして受け取った Blob データ

    Returns:
        None
    """
    blob_name = os.path.basename(myblob.name)
    
    try:
        
        logging.info(f"Processing blob: Name: {blob_name}")

        queue_client = QueueClient(account_url=QUEUE_STORAGE_URI, queue_name=QUEUE_NAME, credential=DefaultAzureCredential())

        queue_client.send_message(content=base64.b64encode(blob_name.encode()).decode())

    except ResourceNotFoundError:
        # Blobが見つからない場合（削除された場合など）
        logging.warning(f"Blob {blob_name} not found. Skipping.")
    except Exception as e:
        logging.error(f"Error occurred while processing {blob_name}: {e}")
        
@app.timer_trigger(schedule="0 0 * * * *", 
              arg_name="timer",
              run_on_startup=False) 
def check_deleted_blobs(timer: func.TimerRequest) -> None:
    """
    削除されたBLOBファイルをチェックする関数です。タイマーイベントで起動します。

    Parameters:
        timer (func.TimerRequest): タイマーイベントの情報を含むオブジェクト

    Returns:
        None
    """
    logging.info('check_deleted_blobs function started.')
    try:

        blob_processor = BlobDocumentProcessor(blob_manager, parser, embedder, mapping_manager)
        
        blob_service_client = BlobServiceClient(account_url=BLOB_STORAGE_URI, credential=DefaultAzureCredential())
        doc_container_client = blob_service_client.get_container_client(container=DOC_CONTAINER_NAME)
        db_container_client = blob_service_client.get_container_client(container=DB_CONTAINER_NAME)

        # 前回起動時に退避しておいたBLOBファイル一覧を取得
        indexed_blob_names = load_indexed_blob_snapshot(db_container_client)
        
        # スナップショットの作成
        snapshot_blob_files(doc_container_client, db_container_client)

        # 前回起動時には存在していたが、現状存在しないBLOBファイル名の一覧を作成する
        current_blob_files = doc_container_client.list_blobs()
        current_blob_names = [blob.name for blob in current_blob_files]
        deleted_blob_names = list(set(indexed_blob_names) - set(current_blob_names))
        logging.info(f"Number of deleted files: {str(len(deleted_blob_names))}")

        for deleted_blob in deleted_blob_names:
            logging.info(f"Deleted blob: {deleted_blob}")
            blob_processor.delete_document(DOC_CONTAINER_NAME, deleted_blob)
            
            logging.info(f"Deleted {deleted_blob} from the index.")

    except FileNotFoundError:
        # indexed_blob_snapshot.csv が見つからない場合
        # 削除処理は実行せず、スナップショットの作成だけを行う
        logging.error("indexed_blob_snapshot.csv not found in the DB container.")
        snapshot_blob_files(doc_container_client, db_container_client)
        logging.info("Created indexed_blob_snapshot.csv.")

    except Exception as e:
        logging.error(f"Error occurred in check_deleted_blobs: {e}")

@app.queue_trigger(arg_name="azqueue", queue_name=f"{QUEUE_NAME}",
                   connection="QueueStorageConnection")
def index_queued_blobs(azqueue: func.QueueMessage) -> None:
    """キューにあるBlobを1つずつインデクシングします

    CosmosDBを使用しない場合は、BLOBに格納されているインデックスファイルへ書き込みを行う際に
    排他エラーが発生する可能性があるため、必ずhost.jsonのbatch_sizeが1になっていることを確認してください。
    
    Args:
        azqueue (func.QueueMessage): キューから取得したメッセージ。

    Returns:
        None
    """

    try:
        # キューからBlob名を取得
        queue_item = azqueue.get_body().decode('UTF-8')
        doc_blobname = str(queue_item)
        
        # Blobのメタデータを取得
        metadata = blob_manager.get_metadata(DOC_CONTAINER_NAME, doc_blobname)
        
        # 'processed'メタデータが存在し、その値が'true'の場合、処理をスキップ
        if metadata.get('processed') == 'true':
            logging.info(f"Blob {doc_blobname} already processed. Skipping.")
            return

        blob_processor = BlobDocumentProcessor(blob_manager, parser, embedder, mapping_manager)
        
        blob_processor.process_and_save_document(DOC_CONTAINER_NAME, doc_blobname)
        
        # 処理済みマーカーをメタデータとして設定
        blob_manager.add_metadata(DOC_CONTAINER_NAME, doc_blobname, {'processed': 'true'})
    
    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"Error occurred: {e}, traceback: {tb}")
    
    return

@app.route(route="search/vector", methods=[func.HttpMethod.GET, func.HttpMethod.POST])
def search_vector(req: func.HttpRequest) -> func.HttpResponse:
    
    try:
        # 検索クエリを取得
        query = req.params.get("query")
        max_results = req.params.get("max_results")
        if not max_results:
            max_results = "5"
            
        logging.info(f"Search query: {query}, max_results: {max_results}")

        logging.info("Searching...")    

        vector_search = VectorSearch(blob_manager, embedder, DB_CONTAINER_NAME, 'indexes/vector_index')
        search_results = vector_search.search(query, k=int(max_results))

        logging.info(f"Search results: {search_results}")

        # Return the search results as a JSON response
        return func.HttpResponse(json.dumps(search_results), mimetype="application/json")

    except Exception as e:
        logging.error(f"Error occurred during search: {e}")
        return func.HttpResponse("An error occurred during search", status_code=500)
    
@app.route(route="search/keyword", methods=[func.HttpMethod.GET, func.HttpMethod.POST])
def search_keyword(req: func.HttpRequest) -> func.HttpResponse:
    
    try:
        # 検索クエリを取得
        query = req.params.get("query")
        max_results = req.params.get("max_results")
        if not max_results:
            max_results = "5"
            
        logging.info(f"Search query: {query}, max_results: {max_results}")

        logging.info("Searching...")    

        keyword_search = KeywordSearch(blob_manager, DB_CONTAINER_NAME, 'indexes/keyword_index')
        search_results = keyword_search.search(query, k=int(max_results))

        logging.info(f"Search results: {search_results}")

        # Return the search results as a JSON response
        return func.HttpResponse(json.dumps(search_results), mimetype="application/json")

    except Exception as e:
        logging.error(f"Error occurred during search: {e}")
        return func.HttpResponse("An error occurred during search", status_code=500)

def snapshot_blob_files(doc_container_client: ContainerClient, db_container_client: ContainerClient) -> None:
    """
    BLOBに格納されているファイル一覧をCSVファイルとしてDBコンテナに保存します。

    Args:
        doc_container_client (ContainerClient): ドキュメントコンテナにアクセスするために使用するコンテナクライアント。
        db_container_client (ContainerClient): DBコンテナにアクセスするために使用するコンテナクライアント。

    Returns:
        None
    """

    try:
        # 現在のBLOBに格納されているファイル一覧を取得
        indexed_blobs = doc_container_client.list_blobs()
        indexed_blob_names = [blob.name for blob in indexed_blobs]
        logging.info(f"Indexed blobs: {indexed_blob_names}")

        # BLOBに格納されているファイル一覧をCSVファイルとしてDBコンテナに保存する
        # ファイル名は「indexed_blob_snapshot.csv」
        indexed_blob_snapshot_buffer = io.StringIO()
        indexed_blob_snapshot_writer = csv.writer(indexed_blob_snapshot_buffer)
        indexed_blob_snapshot_writer.writerow(indexed_blob_names)
        logging.info("Created indexed_blob_snapshot.csv.")

        # 作成したCSVファイルをBLOBにアップロード（上書き）
        # ファイルが存在しない場合は作成する
        blob_client = db_container_client.get_blob_client("indexed_blob_snapshot.csv")
        blob_client.upload_blob(indexed_blob_snapshot_buffer.getvalue(), overwrite=True)
        logging.info("Uploaded indexed_blob_snapshot.csv to the DB container.")

    except Exception as e:
        logging.error(f"Error occurred during snapshot_blob_files: {e}")

    return

def load_indexed_blob_snapshot(db_container_client: ContainerClient) -> List[str]:
    """
    DBコンテナに保存されているBLOBファイル一覧を取得します。

    Args:
        db_container_client (ContainerClient): DBコンテナにアクセスするために使用するコンテナクライアント。

    Returns:
        List[str]: DBコンテナに保存されているBLOBファイル一覧。

    Raises:
        FileNotFoundError: "indexed_blob_snapshot.csv" が見つからない場合
        Exception: その他のエラーが発生した場合
    """

    try:
        # DBコンテナに保存されているBLOBファイル一覧を取得
        if not db_container_client.get_blob_client("indexed_blob_snapshot.csv").exists():
            raise FileNotFoundError
        
        blob_client = db_container_client.get_blob_client("indexed_blob_snapshot.csv")
        blob_data = blob_client.download_blob().readall().decode()

        # CSVリーダーを使用してBLOBファイル一覧を読み込み
        csv_reader = csv.reader(io.StringIO(blob_data))
        indexed_blob_names = next(csv_reader)  # 最初の行を読み込む
        logging.info(f"Loaded indexed_blob_snapshot.csv: {indexed_blob_names} ...")

    except FileNotFoundError:
        logging.error("indexed_blob_snapshot.csv not found in the DB container.")
        raise  # FileNotFoundError を再スローする

    except Exception as e:
        logging.error(f"Error occurred during load_indexed_blob_snapshot: {e}")
        raise  # その他の例外を再スローさせる

    return indexed_blob_names