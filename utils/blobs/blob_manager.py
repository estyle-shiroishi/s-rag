import functools
from typing import Callable, Any, Union, Dict, Optional, List, Tuple
from azure.storage.blob import ContainerClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
import logging

from .blob_container_manager import BlobContainerManager

class BlobManager:
    """
    Azure Blob Storageを管理するためのクラス。

    このクラスは、Azure Blob Storageとのインタラクションを抽象化し、
    データの読み書き、削除、メタデータの操作などの操作を簡単に行えるようにします。
    また、適切なリース管理を行い、並行アクセスの問題を防ぎます。
    さらに、トランザクション的な操作をサポートします。

    Attributes:
        __container_manager (BlobContainerManager): Blobコンテナを管理するためのインスタンス

    使用例:
        # 接続文字列を使用する場合
        blob_manager = BlobManager(connection_string="your_connection_string")

        # account_urlとcredentialを使用する場合
        from azure.identity import DefaultAzureCredential
        blob_manager = BlobManager(account_url="https://your_account.blob.core.windows.net", credential=DefaultAzureCredential())

        # データを書き込む
        blob_manager.upload("my-container", "hello.txt", "Hello, World!")

        # データを読み込む
        data = blob_manager.read("my-container", "hello.txt")
        
        # バイトデータとして読み込む
        byte_data = blob_manager.read("my-container", "hello.txt", as_byte=True)

        # データを削除する
        blob_manager.delete("my-container", "hello.txt")

        # メタデータを追加する
        blob_manager.add_metadata("my-container", "hello.txt", {"key": "value"})

        # メタデータを取得する
        metadata = blob_manager.get_metadata("my-container", "hello.txt")

        # Blobの存在を確認する
        exists = blob_manager.blob_exist("my-container", "hello.txt")

        # コンテナの存在を確認する
        container_exists = blob_manager.container_exist("my-container")

        # トランザクション的な操作を実行する
        # トランザクション的な操作を実行する
        def upload_operation(container_client: ContainerClient):
            self._transaction_upload(container_client, "hello.txt", "Hello, World!")

        def delete_operation(container_client: ContainerClient):
            self._transaction_delete(container_client, "hello.txt")

        operations = [upload_operation, delete_operation]
        success = self.transaction("my-container", operations)
    """

    def __init__(self, connection_string: Optional[str] = None, account_url: Optional[str] = None, credential: Any = None):
        """
        BlobManagerのインスタンスを初期化します。

        Args:
            connection_string (Optional[str]): Azure Storage接続文字列
            account_url (Optional[str]): BlobストレージのアカウントURL
            credential (Any): 認証情報（DefaultAzureCredentialなど）
        """
        self.__container_manager = BlobContainerManager(
            connection_string=connection_string,
            account_url=account_url,
            credential=credential
        )

    def with_blob_lease(method: Callable) -> Callable:
        """
        Blobのリースを取得し、メソッド実行後にリースを解放するデコレーター。

        Args:
            method (Callable): デコレートするメソッド

        Returns:
            Callable: ラップされたメソッド
        """
        @functools.wraps(method)
        def wrapper(self, container_name: str, blob_name: str, *args, **kwargs):
            blob_client, lease_id = self.__container_manager.acquire_lease(container_name, blob_name)
            try:
                kwargs['blob_client'] = blob_client
                kwargs['lease_id'] = lease_id
                return method(self, container_name, blob_name, *args, **kwargs)
            except Exception as e:
                logging.error(f"Error occurred during execution with lease. Container: {container_name}, Blob: {blob_name}. Error: {str(e)}")
                raise
            finally:
                if lease_id:
                    self.__container_manager.release_lease(container_name, blob_name)
        return wrapper
    
    def read(self, container_name: str, blob_name: str, as_byte: bool = True) -> Union[bytes, str]:
        """
        指定されたコンテナとBlobからデータを読み込みます。
        この操作はリースを取得せずに行われます。

        Args:
            container_name (str): コンテナ名
            blob_name (str): Blob名
            as_byte (bool, optional): Trueの場合、データをバイトとして返します。Falseの場合、文字列として返します。デフォルトはFalse。

        Returns:
            Union[bytes, str]: 読み込んだデータ。

        Raises:
            ResourceNotFoundError: Blobが存在しない場合
            Exception: データの読み込みに失敗した場合
        """
        try:
            blob_client = self.__container_manager.get_client(container_name, blob_name, create_if_not_exists=False)
            data = blob_client.download_blob().readall()
            if as_byte:
                return data
            return data.decode('utf-8')
        except ResourceNotFoundError:
            logging.warning(f"Blob '{blob_name}' not found in container '{container_name}'")
            raise
        except Exception as e:
            logging.error(f"Failed to read blob '{blob_name}' from container '{container_name}': {str(e)}")
            raise

    @with_blob_lease
    def upload(self, container_name: str, blob_name: str, data: Union[str, bytes, None] = None, blob_client: BlobClient = None, lease_id: str = None, overwrite: bool = True) -> None:
        """
        指定されたコンテナとBlobにデータを書き込みます。
        この操作はリースを取得して行われます。
        データが渡されなかった場合、空のファイルを作成します。

        Args:
            container_name (str): コンテナ名
            blob_name (str): Blob名
            data (Union[str, bytes, None], optional): アップロードするデータ。デフォルトはNone。
            blob_client (BlobClient, optional): Blobクライアント
            lease_id (str, optional): リースID
            overwrite (bool, optional): 上書きを許可するかどうか。デフォルトはTrue。

        Returns:
            None
        """
        if data is None:
            data = b''  
        elif isinstance(data, str):
            data = data.encode('utf-8')
        
        blob_client.upload_blob(data, overwrite=overwrite, lease=lease_id)
        logging.info(f"Successfully wrote data to blob '{blob_name}' in container '{container_name}'")

    @with_blob_lease
    def delete(self, container_name: str, blob_name: str, blob_client: BlobClient = None, lease_id: str = None) -> None:
        """
        指定されたコンテナとBlobを削除します。
        この操作はリースを取得して行われます。

        Args:
            container_name (str): コンテナ名
            blob_name (str): Blob名
            blob_client (BlobClient, optional): Blobクライアント
            lease_id (str, optional): リースID

        Returns:
            None
        """
        blob_client.delete_blob(lease=lease_id)
        logging.info(f"Successfully deleted blob '{blob_name}' from container '{container_name}'")
        
    @with_blob_lease
    def add_metadata(self, container_name: str, blob_name: str, metadata: Dict[str, str], blob_client: BlobClient = None, lease_id: str = None) -> bool:
        """
        指定されたblobにメタデータを追加します。
        この操作はリースを取得して行われます。

        Args:
            container_name (str): コンテナ名
            blob_name (str): Blob名
            metadata (Dict[str, str]): 追加するメタデータ
            blob_client (BlobClient, optional): Blobクライアント
            lease_id (str, optional): リースID

        Returns:
            bool: メタデータの追加に成功した場合はTrue
        """
        properties = blob_client.get_blob_properties()
        existing_metadata = properties.metadata if properties.metadata else {}
        existing_metadata.update(metadata)
        blob_client.set_blob_metadata(metadata=existing_metadata, lease=lease_id)
        logging.info(f"Metadata added successfully. Blob: {blob_name}, Container: {container_name}")
        return True

    def get_metadata(self, container_name: str, blob_name: str) -> Dict[str, str]:
        """
        指定されたコンテナとBlobのメタデータを取得します。
        この操作はリースを取得せずに行われます。

        Args:
            container_name (str): コンテナ名
            blob_name (str): Blob名

        Returns:
            Dict[str, str]: Blobのメタデータ。

        Raises:
            ResourceNotFoundError: Blobが存在しない場合
            AzureError: メタデータの取得に失敗した場合
        """
        try:
            blob_client = self.__container_manager.get_client(container_name, blob_name)
            properties = blob_client.get_blob_properties()
            return properties.metadata
        except ResourceNotFoundError:
            logging.warning(f"Blob '{blob_name}' not found in container '{container_name}'")
            raise
        except AzureError as e:
            logging.error(f"Failed to get metadata for blob '{blob_name}' in container '{container_name}': {str(e)}")
            raise

    def blob_exist(self, container_name: str, blob_name: str) -> bool:
        """
        指定されたコンテナ内のblobが存在するかどうかを確認します。
        この操作はリースを取得して行われます。

        Args:
            container_name (str): コンテナの名前
            blob_name (str): 確認するblobの名前
            blob_client (BlobClient, optional): Blobクライアント

        Returns:
            bool: blobが存在する場合はTrue、存在しない場合はFalse
        """
        try:
            blob_client = self.__container_manager.get_client(container_name, blob_name, create_if_not_exists=False)
            blob_client.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False

    def container_exist(self, container_name: str) -> bool:
        """
        指定されたコンテナが存在するかどうかを確認します。
        この操作はリースを取得して行われます。

        Args:
            container_name (str): 確認するコンテナの名前
            container_client (ContainerClient, optional): コンテナクライアント

        Returns:
            bool: コンテナが存在する場合はTrue、存在しない場合はFalse
        """
        try:
            container_client = self.__container_manager.get_client(container_name, create_if_not_exists=False)
            container_client.get_container_properties()
            return True
        except ResourceNotFoundError:
            return False

    def list_blobs(self, container_name: str) -> List[str]:
        """
        指定されたコンテナ内のすべてのBlobをリストアップします。
        この操作はコンテナのリースを取得して行われます。

        Args:
            container_name (str): コンテナ名
            container_client (ContainerClient, optional): コンテナクライアント

        Returns:
            List[str]: コンテナ内のBlobの名前のリスト
        """
        container_client = self.__container_manager.get_client(container_name, create_if_not_exists=False)
        blobs = container_client.list_blobs()
        return [blob.name for blob in blobs]
    
    @property
    def container_manager(self):
        return self.__container_manager