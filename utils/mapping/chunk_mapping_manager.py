import logging
from typing import Dict, Any, List, Tuple, Optional

from .chunk_index_mapping import ChunkIndexMapping
from .chunk_blob_mapping import ChunkBlobMapping
from utils.blobs.blob_manager import BlobManager
from utils.blobs.blob_container_manager import BlobContainerManager
from utils.searchers.keyword_search import KeywordSearch
from utils.searchers.vector_search import VectorSearch

class ChunkMappingManager:
    """
    チャンク、インデックス、Blobの関係を管理するマネージャー。
    
    このクラスは、チャンクのインデックス管理とBlobストレージの対応関係を
    一元的に管理します。

    使用方法:
    1. 必要なインデックスマネージャーをインポートする
    from utils.searchers.keyword_search import KeywordSearch
    from utils.searchers.vector_search import VectorSearch

    2. 各インデックスマネージャーのインスタンスを作成する
    keyword_search = KeywordSearch(blob_manager, container_name, 'keyword_index.pkl')
    vector_search = VectorSearch(blob_manager, embedding, container_name, 'vector_index.pkl')

    3. ChunkManagerのインスタンスを作成する
    searchers = {
        'keyword': keyword_search,
        'vector': vector_search
    }
    chunk_manager = ChunkManager(blob_manager, searchers)

    4. チャンクを追加する
    chunk_id, text = chunk_manager.add(["これは新しいチャンクです。"], "my-container", "my-blob.txt")[0]

    5. チャンクを削除する
    chunk_manager.remove(chunk_id)
    """

    def __init__(self, blob_manager: BlobManager, searchers: Dict[str, Any],
                 db_container: str = "db-container",
                 chunk_index_mapping_file: str = "mapping/chunk_index_mapping.json",
                 chunk_blob_mapping_file: str = "mapping/chunk_blob_mapping.json"):
        self.logger = logging.getLogger(__name__)
        self.__blob_manager = blob_manager
        self.__searchers = searchers
        self.chunk_id_mapping_manager = ChunkIndexMapping(blob_manager, db_container, chunk_index_mapping_file)
        self.chunk_blob_mapping_manager = ChunkBlobMapping(blob_manager, db_container, chunk_blob_mapping_file)
        self.chunk_index_mapping_file = chunk_index_mapping_file
        self.chunk_blob_mapping_file = chunk_blob_mapping_file
        self.db_container = db_container
        self.container_manager = self.__blob_manager.container_manager
        self.logger.info("ChunkManager initialized")

    def add(self, blob_container: str, blob_name: str, texts: List[str]) -> List[Tuple[str, str]]:
        """
        複数のチャンクを全てのインデックスに追加し、Blobとの対応関係を保存する
        
        :param texts: 追加するチャンクのテキストのリスト
        :param blob_container: チャンクが属するBlobのコンテナ名
        :param blob_name: チャンクが属するBlobの名前
        :return: 生成されたチャンクIDとテキストのタプルのリスト
        """
        index_lease_id = None
        blob_lease_id = None
        try:
            _, index_lease_id = self.container_manager.acquire_lease(self.db_container, self.chunk_index_mapping_file)
            _, blob_lease_id = self.container_manager.acquire_lease(self.db_container, self.chunk_blob_mapping_file)

            added_chunks = []
            for text in texts:
                chunk_id = self.chunk_id_mapping_manager.get_new_id()
                index_ids = {}
                for index_name, index_manager in self.__searchers.items():
                    index_id = index_manager.add(text)[0]
                    index_ids[index_name] = index_id
                self.chunk_id_mapping_manager.add_mapping(chunk_id, index_ids)
                self.chunk_blob_mapping_manager.add_mapping(chunk_id, blob_container, blob_name)
                self.logger.info(f"Chunk added. Chunk ID: {chunk_id}")
                added_chunks.append((chunk_id, text))
            
            return added_chunks

        finally:
            if index_lease_id:
                self.container_manager.release_lease(self.db_container, self.chunk_index_mapping_file)
            if blob_lease_id:
                self.container_manager.release_lease(self.db_container, self.chunk_blob_mapping_file)

    def remove(self, chunk_id: str):
        """
        指定されたチャンクIDのチャンクを全てのインデックスから削除し、Blob対応も削除する
        インデックスが初期化されていない場合や削除に失敗した場合でも、マッピング情報は削除する
        
        :param chunk_id: 削除するチャンクのチャンクID
        """
        index_lease_id = None
        blob_lease_id = None
        try:
            _, index_lease_id = self.container_manager.acquire_lease(self.db_container, self.chunk_index_mapping_file)
            _, blob_lease_id = self.container_manager.acquire_lease(self.db_container, self.chunk_blob_mapping_file)

            index_ids = self.chunk_id_mapping_manager.get_index_ids(chunk_id)
            if index_ids:
                for index_name, index_manager in self.__searchers.items():
                    if index_name in index_ids:
                        try:
                            index_manager.remove(index_ids[index_name])
                        except Exception as e:
                            self.logger.warning(f"Error occurred while removing chunk ID {chunk_id} from index {index_name}: {e}")
            else:
                self.logger.warning(f"Chunk with ID: {chunk_id} not found")

            # マッピング情報は常に削除する
            self.chunk_id_mapping_manager.remove_mapping(chunk_id)
            self.chunk_blob_mapping_manager.remove_mapping(chunk_id)
            self.logger.info(f"Chunk with ID: {chunk_id} has been removed")

        finally:
            if index_lease_id:
                self.container_manager.release_lease(self.db_container, self.chunk_index_mapping_file)
            if blob_lease_id:
                self.container_manager.release_lease(self.db_container, self.chunk_blob_mapping_file)