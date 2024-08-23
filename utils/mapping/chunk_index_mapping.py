import json
import logging
from typing import Dict, Any, Optional
from azure.core.exceptions import ResourceNotFoundError

from utils.blobs.blob_manager import BlobManager

class ChunkIndexMapping:
    """
    チャンクIDと各インデックスのIDをマッピングするクラス。
    
    このクラスは、異なるインデックス間でチャンクIDを関連付けるために使用されます。
    マッピング情報はAzure Blob StorageのJSONファイルに保存され、永続化されます。
    """

    def __init__(self, blob_manager: BlobManager, container_name: str, blob_name: str = "mapping/chunk_index_mapping.json"):
        self.logger = logging.getLogger(__name__)
        self.__blob_manager = blob_manager
        self.__container_name = container_name
        self.__blob_name = blob_name
        self.__load_from_storage()

    def __load_from_storage(self):
        """BlobストレージからチャンクIDマッピング情報を読み込む"""
        try:
            data = self.__blob_manager.read(self.__container_name, self.__blob_name)
            if data:
                json_data = json.loads(data.decode('utf-8'))
                self.__id_counter = json_data.get('id_counter', 0)
                self.__id_map = json_data.get('id_map', {})
            else:
                self.__id_counter = 0
                self.__id_map = {}
                self.__save_to_storage()
        except ResourceNotFoundError:
            self.__id_counter = 0
            self.__id_map = {}
            self.__save_to_storage()
        except json.JSONDecodeError:
            self.logger.error("JSONデコードエラーが発生しました。新しいマッピングを初期化します。")
            self.__id_counter = 0
            self.__id_map = {}
            self.__save_to_storage()
        self.logger.info(f"Loaded chunk ID mapping from {self.__blob_name}")

    def __save_to_storage(self):
        """チャンクIDマッピング情報をBlobストレージに保存する"""
        data = {
            'id_counter': self.__id_counter,
            'id_map': self.__id_map
        }
        json_data = json.dumps(data).encode('utf-8')
        self.__blob_manager.upload(self.__container_name, self.__blob_name, json_data)
        self.logger.debug(f"Saved chunk ID mapping to {self.__blob_name}")

    def get_new_id(self) -> str:
        """新しいチャンクIDを生成し、カウンターをインクリメントします。"""
        self.__load_from_storage()  # 最新の値を読み込む
        new_id = str(self.__id_counter)
        self.__id_counter += 1  # カウンターをインクリメント
        self.__save_to_storage()  # 更新された値を保存
        return new_id

    def add_mapping(self, chunk_id: str, index_ids: Dict[str, Any]):
        """
        チャンクIDとインデックスIDのマッピングを追加します。
        
        :param chunk_id: 追加するチャンクID
        :param index_ids: インデックスIDのディクショナリ
        """
        self.__load_from_storage()  # 最新の値を読み込む
        self.__id_map[chunk_id] = index_ids
        self.__save_to_storage()  # 更新された値を保存
        self.logger.debug(f"Added mapping for chunk ID: {chunk_id}")

    def remove_mapping(self, chunk_id: str):
        """指定されたチャンクIDのマッピングを削除する"""
        self.__load_from_storage()  # 最新の値を読み込む
        if chunk_id in self.__id_map:
            del self.__id_map[chunk_id]
            self.__save_to_storage()  # 更新された値を保存
            self.logger.debug(f"Removed mapping for chunk ID: {chunk_id}")
            
    def get_chunk_id(self, index_name: str, index_id: Any) -> Optional[str]:
        """特定のインデックスのIDに対応するチャンクIDを取得する"""
        self.__load_from_storage()  # 最新の値を読み込む
        for chunk_id, index_ids in self.__id_map.items():
            if index_ids.get(index_name) == index_id:
                return chunk_id
        return None
    
    def get_index_ids(self, chunk_id: str) -> Optional[Dict[str, Any]]:
        """指定されたチャンクIDに対応するインデックスIDのマッピングを取得する"""
        self.__load_from_storage()  # 最新の値を読み込む
        return self.__id_map.get(chunk_id)