import json
import logging
from typing import Dict, Optional, List
from azure.core.exceptions import ResourceNotFoundError

from utils.blobs.blob_manager import BlobManager

class ChunkBlobMapping:
    """
    チャンクIDとBlobの対応関係を管理するクラス。
    
    このクラスは、チャンクIDと対応するBlobの情報（コンテナ名、Blob名）をマッピングします。
    マッピング情報はAzure Blob StorageのJSONファイルに保存され、永続化されます。

    使用方法:
    ```
    from utils.blobs.blob_manager import BlobManager

    # BlobManagerインスタンスを作成
    blob_manager = BlobManager("your_connection_string")

    # BlobMappingManagerインスタンスを作成
    mapping_manager = BlobMappingManager(blob_manager, "db_container", "mapping.json")

    # マッピングを追加
    mapping_manager.add_mapping("1", "doc_container", "test.pdf")
    mapping_manager.add_mapping("2", "doc_container", "test.pdf")

    # Blob情報を取得
    blob_info = mapping_manager.get_blob_info("1")
    print(blob_info)  # 出力: {'container': 'doc_container', 'blob': 'test.pdf'}

    # チャンクIDリストを取得
    chunk_ids = mapping_manager.get_chunk_ids_by_blob("doc_container", "test.pdf")
    print(chunk_ids)  # 出力: ['1', '2']

    # マッピングを削除
    mapping_manager.remove_mapping("1")
    ```

    マッピング情報は自動的にBlobストレージに保存され、インスタンス作成時に読み込まれます。
    """

    def __init__(self, blob_manager: BlobManager, container_name: str, blob_name: str = "mapping/chunk_blob_mapping.json"):
        self.logger = logging.getLogger(__name__)
        self.__blob_manager = blob_manager
        self.__container_name = container_name
        self.__blob_name = blob_name
        self.__mapping = {}
        self.__reverse_mapping = {}
        self.__load_from_storage()

    def __load_from_storage(self):
        """Blobストレージからチャンクblobマッピング情報を読み込む"""
        try:
            data = self.__blob_manager.read(self.__container_name, self.__blob_name)
            if data:
                json_data = json.loads(data) if isinstance(data, str) else json.loads(data.decode('utf-8'))
                self.__mapping = json_data
                self.__update_reverse_mapping()
            else:
                self.__mapping = {}
                self.__reverse_mapping = {}
        except (ResourceNotFoundError, json.JSONDecodeError):
            self.__mapping = {}
            self.__reverse_mapping = {}
            self.__save_to_storage()
        self.logger.info(f"Loaded chunk-blob mapping from {self.__blob_name}")

    def __save_to_storage(self):
        """チャンクblobマッピング情報をBlobストレージに保存する"""
        json_data = json.dumps(self.__mapping).encode('utf-8')
        self.__blob_manager.upload(self.__container_name, self.__blob_name, json_data)
        self.logger.debug(f"Saved chunk-blob mapping to {self.__blob_name}")

    def __update_reverse_mapping(self):
        """逆マッピング（Blob名からチャンクID）を更新する"""
        self.__reverse_mapping = {}
        for chunk_id, blob_info in self.__mapping.items():
            key = f"{blob_info['container']}:{blob_info['blob']}"
            if key not in self.__reverse_mapping:
                self.__reverse_mapping[key] = []
            self.__reverse_mapping[key].append(chunk_id)

    def add_mapping(self, chunk_id: str, blob_container: str, blob_name: str):
        """チャンクIDとBlobのマッピングを追加する"""
        self.__mapping[chunk_id] = {"container": blob_container, "blob": blob_name}
        self.__update_reverse_mapping()
        self.__save_to_storage()
        self.logger.debug(f"Added mapping for chunk ID: {chunk_id}")

    def remove_mapping(self, chunk_id: str):
        """指定されたチャンクIDのマッピングを削除する"""
        if chunk_id in self.__mapping:
            del self.__mapping[chunk_id]
            self.__update_reverse_mapping()
            self.__save_to_storage()
            self.logger.debug(f"Removed mapping for chunk ID: {chunk_id}")

    def get_blob_info(self, chunk_id: str) -> Optional[Dict[str, str]]:
        """チャンクIDに対応するBlob情報を取得する"""
        return self.__mapping.get(chunk_id)
    
    def get_chunk_ids_by_blob(self, container_name: str, blob_name: str) -> List[str]:
        """指定されたコンテナ名とBlob名に対応するチャンクIDのリストを取得する"""
        key = f"{container_name}:{blob_name}"
        return self.__reverse_mapping.get(key, [])