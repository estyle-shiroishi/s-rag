import logging
import json
from typing import Any, List, Dict, Optional, Union

from azure.core.exceptions import ResourceNotFoundError

from ..blobs.blob_manager import BlobManager
from ..indexes.bm25_index_manager import BM25IndexManager
from ..mapping.chunk_blob_mapping import ChunkBlobMapping
from ..mapping.chunk_index_mapping import ChunkIndexMapping

class KeywordSearch:
    """
    キーワード検索を行うクラス。

    このクラスは、与えられたクエリに対してキーワード検索を実行し、
    最も関連性の高いドキュメントを返します。検索結果には、チャンクと全文の情報も含まれます。

    使用例:
    # BlobManagerの初期化
    from azure.identity import DefaultAzureCredential
    blob_manager = BlobManager(account_url="https://your_account.blob.core.windows.net", credential=DefaultAzureCredential())
    
    # KeywordSearchオブジェクトの作成
    keyword_search = KeywordSearch(blob_manager, "your_container_name", "index_blob_name")
    
    # ドキュメントの追加
    docs = ["これは最初のドキュメントです。", "2番目のドキュメントはこちらです。"]
    keyword_search.add(docs)
    
    # 検索の実行
    results = keyword_search.search("ドキュメント", k=2)
    for result in results:
        print(f"ドキュメントID: {result['id']}, スコア: {result['score']}")
        print(f"チャンク: {result['chunk']}")
        print(f"全文: {result['full_text'][:100]}...")  # 全文の最初の100文字を表示

    属性:
        keyword_search (BM25IndexManager): キーワードインデックスを管理するオブジェクト
    """

    def __init__(self, blob_manager: BlobManager, container_name: str, blob_name: str, 
                 chunk_blob_mapping_name: str = "mapping/chunk_blob_mapping.json", 
                 chunk_index_mapping_name: str = "mapping/chunk_index_mapping.json", 
                 **kwargs):
        self.logger = logging.getLogger(__name__)
        self.blob_manager = blob_manager
        self.container_name = container_name
        self.blob_name = blob_name
        self.keyword_index = self.__load_or_create_index(self.blob_manager, self.container_name, self.blob_name, **kwargs)
        self.chunk_blob_mapping = ChunkBlobMapping(blob_manager, container_name, chunk_blob_mapping_name)
        self.chunk_index_mapping = ChunkIndexMapping(blob_manager, container_name, chunk_index_mapping_name)

    def __load_or_create_index(self, blob_manager: BlobManager, container_name: str, blob_name: str, **kwargs):
        try:
            index_data = blob_manager.read(container_name, blob_name, as_byte=True)
            if index_data:
                return BM25IndexManager.load_from_byte(index_data)
            else:
                self.logger.warning(f"Blob '{blob_name}' in container '{container_name}' is empty. Creating new index.")
                return BM25IndexManager(**kwargs)
        except ResourceNotFoundError:
            self.logger.warning(f"Blob '{blob_name}' not found in container '{container_name}'. Creating new index.")
            return BM25IndexManager(**kwargs)

    def add(self, texts: List[str], ids: Optional[List[Any]] = None) -> List[Any]:
        """
        キーワードインデックスに新しい文書を追加します。追加後、自動的に保存します。

        引数:
            texts (List[str]): 追加する文書のリスト
            ids (Optional[List[Any]]): テキストに対応するIDのリスト（省略可能）

        戻り値:
            List[Any]: 追加された文書のID
        """
        if ids is None:
            added_ids = self.keyword_index.add(texts)
        else:
            added_ids = self.keyword_index.add(texts, ids)
        
        self.__save()
        return added_ids

    def remove(self, ids: Union[int, List[int]]):
        """
        指定されたIDの文書をインデックスから削除します。

        引数:
            ids (Union[int, List[int]]): 削除する文書のIDまたはIDのリスト

        戻り値:
            None
        """
        self.keyword_index.remove(ids)
        self.__save()
        
    def restore(self, ids: Union[int, List[int]]):
        """
        指定されたIDの文書を復元します。

        引数:
            ids (Union[int, List[int]]): 復元する文書のIDまたはIDのリスト

        戻り値:
            None
        """
        self.keyword_index.unmark_deleted(ids)
        self.__save()
        self.logger.info(f"Documents {ids} have been restored.")

    def search(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """
        与えられたクエリに対してキーワード検索を実行し、結果にチャンクと全文を追加します。

        引数:
            query (str): 検索クエリ
            k (int): 返す結果の数

        戻り値:
            List[Dict[str, Any]]: 検索結果のリスト。各要素は以下の形式のディクショナリです：
            {
                'id': Any,  # ドキュメントID
                'score': float,  # 関連性スコア
                'chunk': str,  # チャンクのテキスト
                'full_text': str  # 全文のテキスト
            }
        """
        keyword_ids, keyword_scores = self.keyword_index.search(query, k=k)
        
        results = []
        for doc_id, score in zip(keyword_ids, keyword_scores):
            chunk_id = self.chunk_index_mapping.get_chunk_id('keyword', doc_id)
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
        キーワードインデックスをBlobストレージに保存します。
        """
        try:
            index_data = self.keyword_index.export()
            self.blob_manager.upload(self.container_name, self.blob_name, index_data)
            self.logger.info(f"Index saved to '{self.container_name}/{self.blob_name}'.")
        except Exception as e:
            self.logger.error(f"Error occurred while saving index: {str(e)}")
            raise