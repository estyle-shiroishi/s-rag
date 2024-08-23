import logging
import json
from typing import Any, List, Dict, Optional, Union

from azure.core.exceptions import ResourceNotFoundError

from ..azure_embedder import AzureEmbedder
from ..blobs.blob_manager import BlobManager
from ..indexes.voyager_index_manager import VoyagerIndexManager
from ..mapping.chunk_blob_mapping import ChunkBlobMapping
from ..mapping.chunk_index_mapping import ChunkIndexMapping

class VectorSearch:
    """
    ベクトル検索を行うクラス。

    このクラスは、与えられたクエリに対してベクトル検索を実行し、
    最も類似度の高いドキュメントを返します。検索結果には、チャンクと全文の情報も含まれます。

    使用例:
    # 必要なオブジェクトを初期化
    from azure.identity import DefaultAzureCredential
    blob_manager = BlobManager(account_url="https://your_account.blob.core.windows.net", credential=DefaultAzureCredential())
    
    embedding = AzureEmbedder(deployment_name, api_key, api_base)

    # VectorSearchオブジェクトを作成
    vector_search = VectorSearch(blob_manager, embedding, container_name, "index.bin")

    # テキストデータをインデックスに追加
    texts = ["これは最初のドキュメントです。", "2番目のドキュメントです。", "3つ目のドキュメントです。"]
    vector_search.add(texts)

    # 検索を実行
    query = "ドキュメントを探しています"
    results = vector_search.search(query, k=2)

    # 結果を表示
    for result in results:
        print(f"ドキュメントID: {result['id']}, スコア: {result['score']}")
        print(f"チャンク: {result['chunk']}")
        print(f"全文: {result['full_text'][:100]}...")  # 全文の最初の100文字を表示
    """

    def __init__(self, blob_manager: BlobManager, embedding: AzureEmbedder, container_name: str, blob_name: str, 
                 chunk_blob_mapping_name: str = "mapping/chunk_blob_mapping.json", 
                 chunk_index_mapping_name: str = "mapping/chunk_index_mapping.json", 
                 **kwargs):
        self.logger = logging.getLogger(__name__)
        self.blob_manager = blob_manager
        self.container_name = container_name
        self.blob_name = blob_name
        self.vector_index = self.__load_or_create_index(self.blob_manager, self.container_name, self.blob_name, **kwargs)
        self.embedding = embedding
        self.chunk_blob_mapping = ChunkBlobMapping(blob_manager, container_name, chunk_blob_mapping_name)
        self.chunk_index_mapping = ChunkIndexMapping(blob_manager, container_name, chunk_index_mapping_name)

    def __load_or_create_index(self, blob_manager: BlobManager, container_name: str, blob_name: str, **kwargs):
        try:
            index_data = blob_manager.read(container_name, blob_name, as_byte=True)
            return VoyagerIndexManager.load_from_byte(index_data)
        except ResourceNotFoundError:
            self.logger.warning(f"Blob '{blob_name}' not found in container '{container_name}'. Creating new index.")
            return VoyagerIndexManager(**kwargs)

    def add(self, texts: List[str], ids: Optional[List[Any]] = None) -> List[Any]:
        """
        テキストをベクトル化し、インデックスに追加します。追加後、自動的に保存します。

        引数:
            texts (List[str]): 追加するテキストのリスト
            ids (Optional[List[Any]]): テキストに対応するIDのリスト（省略可能）

        戻り値:
            List[Any]: 追加されたテキストのID
        """
        embeddings = self.embedding.embed_single(texts)
        if ids is None:
            added_ids = self.vector_index.add(embeddings)
        else:
            added_ids = self.vector_index.add(embeddings, ids)
        
        self.__save()
        return added_ids

    def remove(self, ids: Union[int, List[int]]):
        """
        指定されたIDのベクトルをインデックスから削除します。

        引数:
            ids (Union[int, List[int]]): 削除するベクトルのIDまたはIDのリスト

        戻り値:
            None
        """
        self.vector_index.remove(ids)
        self.__save()
        
    def restore(self, ids: Union[int, List[int]]):
        """
        指定されたIDのベクトルを復元します。

        引数:
            ids (Union[int, List[int]]): 復元するベクトルのIDまたはIDのリスト

        戻り値:
            None
        """
        self.vector_index.unmark_deleted(ids)
        self.__save()
        self.logger.info(f"Vectors {ids} have been restored.")

    def search(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """
        与えられたクエリに対してベクトル検索を実行し、結果にチャンクと全文を追加します。

        引数:
            query (str): 検索クエリ
            k (int): 返す結果の数

        戻り値:
            List[Dict[str, Any]]: 検索結果のリスト。各要素は以下の形式のディクショナリです：
            {
                'id': Any,  # ドキュメントID
                'score': float,  # 類似度スコア
                'chunk': str,  # チャンクのテキスト
                'full_text': str  # 全文のテキスト
            }
        """
        query_embedding = self.embedding.embed_single(query)
        vector_ids, vector_distances = self.vector_index.search(query_embedding, k=k)
        
        results = []
        for doc_id, score in zip(vector_ids, vector_distances):
            chunk_id = self.chunk_index_mapping.get_chunk_id('vector', doc_id)
            if chunk_id:
                chunk_data = json.loads(self.blob_manager.read(self.container_name, f'chunks/chunk_{chunk_id}.json'))
                chunk_text = chunk_data.get('text', '')
                blob_info = self.chunk_blob_mapping.get_blob_info(chunk_id)
                document_name = blob_info.get('blob', 'ファイルが見つかりません') if blob_info else 'ファイルが見つかりません'
                page_number = chunk_data.get('page_number', '')
                
                results.append({
                    'id': int(doc_id), 
                    'score': float(score),  
                    'chunk': chunk_text,
                    'document_name': document_name,
                    'page_number': int(page_number) if page_number else '',  
                })
        
        return results
    
    def __save(self):
        """
        ベクトルインデックスをBlobストレージに保存します。
        """
        try:
            index_data = self.vector_index.export()
            self.blob_manager.upload(self.container_name, self.blob_name, index_data)
            self.logger.info(f"Index saved to '{self.container_name}/{self.blob_name}'.")
        except Exception as e:
            self.logger.error(f"Error occurred while saving index: {str(e)}")
            raise