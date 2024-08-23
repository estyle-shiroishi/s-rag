from datetime import datetime, timedelta
import logging
from typing import Tuple, Dict, Optional, Union
import time

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient, BlobLeaseClient
from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential

from .blob_lease_manager import BlobLeaseManager

class BlobContainerManager:
    """
    Azure Blob Storageのコンテナとblobを管理するためのクラスです。
    
    このクラスは以下の機能を提供します：
    - BlobServiceClientの作成（接続文字列またはアカウントURLと認証情報を使用）
    - コンテナまたはblobの取得または作成
    - コンテナまたはblobのリースの取得と解放
    
    使用例：
    # 接続文字列を使用してBlobServiceClient取得
    manager = BlobContainerManager(connection_string=connection_string)
    
    # コンテナのBlobContainerClient取得又は作成
    container_client = manager.get_client(container_name)
    
    # blobのBlobClient取得
    blob_client = manager.get_client(container_name, blob_name)
    
    # コンテナのリース取得
    container_client, lease_id = manager.acquire_lease(container_name)
    
    # blobのリース取得
    blob_client, lease_id = manager.acquire_lease(container_name, blob_name)
    
    # リース解放
    manager.release_lease(container_name)
    manager.release_lease(container_name, blob_name)
    
    # すべてのリースを解放
    manager.release_all_leases()
    """
    
    def __init__(self, connection_string: str = None, account_url: str = None, credential = None):
        """
        BlobContainerManagerのインスタンスを初期化します。

        Args:
            connection_string (str, optional): Azure Storage接続文字列
            account_url (str, optional): BlobストレージのアカウントURL
            credential (Any, optional): 認証情報（DefaultAzureCredentialなど）
        """
        if connection_string:
            self.__service_client = self.__get_service_client_from_connection_string(connection_string)
        elif account_url and credential:
            self.__service_client = self.__get_service_client_from_credential(account_url, credential)
        else:
            raise ValueError("Either connection_string or both account_url and credential must be provided")
        
        self.__leases: Dict[str, str] = {}  # キー: リソース識別子, 値: リースID
        
    def __del__(self):
        """
        オブジェクトが破棄される際に呼び出され、すべてのリースを解放します。
        """
        self.release_all_leases()
        
    def __get_service_client_from_connection_string(self, connection_string: str) -> BlobServiceClient:
        """
        接続文字列からBlobServiceClientインスタンスを取得します。

        Args:
            connection_string (str): Azure Storage接続文字列

        Returns:
            BlobServiceClient: BlobServiceClientインスタンス
        """
        try:
            service_client = BlobServiceClient.from_connection_string(connection_string)
            logging.info('BlobServiceClient created successfully from connection string.')
            return service_client
        except AzureError as e:
            logging.error(f"Failed to create BlobServiceClient from connection string: {str(e)}")
            raise

    def __get_service_client_from_credential(self, account_url: str, credential) -> BlobServiceClient:
        """
        アカウントURLと認証情報からBlobServiceClientインスタンスを取得します。
        認証に失敗した場合、一度だけ再試行します。

        Args:
            account_url (str): BlobストレージのアカウントURL
            credential: 認証情報（DefaultAzureCredentialなど）

        Returns:
            BlobServiceClient: BlobServiceClientインスタンス
        """
        for attempt in range(2):  # 最初の試行と1回の再試行
            try:
                service_client = BlobServiceClient(account_url=account_url, credential=credential)
                logging.info('BlobServiceClient created successfully from account URL and credential.')
                return service_client
            except AzureError as e:
                if attempt == 0:
                    logging.warning(f"Authentication failed. Retrying: {str(e)}")
                    time.sleep(1)  # 1秒待機してから再試行
                    credential=DefaultAzureCredential()
                else:
                    logging.error(f"Failed to create BlobServiceClient after 2 attempts: {str(e)}")
                    raise
    
    def get_client(self, container_name: str, blob_name: str = None, max_retries: int = 12, retry_delay: int = 5, create_if_not_exists: bool = True) -> Union[ContainerClient, BlobClient]:
        """
        指定されたコンテナのContainerClientまたはblobのBlobClientを取得または作成します。

        Args:
            container_name (str): コンテナの名前
            blob_name (str, optional): blobの名前。指定しない場合はコンテナのクライアントを返します。
            max_retries (int, optional): 最大再試行回数。デフォルトは5回。
            retry_delay (int, optional): 再試行間の待機時間（秒）。デフォルトは20秒。
            create_if_not_exists (bool, optional): クライアントが存在しない場合に作成するかどうか。デフォルトはTrue。

        Returns:
            Union[ContainerClient, BlobClient]: 成功した場合はContainerClientまたはBlobClientインスタンス

        Raises:
            AzureError: Azure Storageとの通信中にエラーが発生した場合
            ResourceNotFoundError: コンテナまたはblobが存在せず、create_if_not_existsがFalseの場合
        """
        for attempt in range(max_retries):
            try:
                container_client = self.__service_client.get_container_client(container_name)
                container_client.get_container_properties()  # コンテナの存在確認
            except ResourceNotFoundError:
                if create_if_not_exists:
                    # コンテナが存在しない場合、ここで作成を試みる
                    try:
                        container_client = self.__service_client.create_container(container_name)
                        logging.info(f"Container '{container_name}' created successfully.")
                    except AzureError as e:
                        if "ContainerBeingDeleted" in str(e):
                            if attempt < max_retries - 1:
                                logging.warning(f"Container '{container_name}' is being deleted. Attempt {attempt + 1}/{max_retries}. Waiting {retry_delay} seconds before retry.")
                                time.sleep(retry_delay)
                                continue  # 次の試行へ
                            else:
                                logging.error(f"Failed to create container '{container_name}' after {max_retries} attempts.")
                                raise ResourceNotFoundError(f"Container '{container_name}' not found or could not be created.")
                        else:
                            logging.error(f"Failed to create container '{container_name}': {str(e)}")
                            raise
                else:
                    raise ResourceNotFoundError(f"Container '{container_name}' not found.")

            if blob_name:
                blob_client = container_client.get_blob_client(blob_name)
                try:
                    blob_client.get_blob_properties()
                except ResourceNotFoundError:
                    if create_if_not_exists:
                        blob_client.upload_blob(data="", overwrite=True)
                        logging.info(f"Blob '{blob_name}' created in container '{container_name}'.")
                    else:
                        raise ResourceNotFoundError(f"Blob '{blob_name}' not found in container '{container_name}'.")
                return blob_client
            else:
                return container_client

        logging.error(f"Failed to get or create container '{container_name}' after {max_retries} attempts.")
        raise ResourceNotFoundError(f"Container '{container_name}' not found or could not be created.")

    def acquire_lease(self, container_name: str, blob_name: str = None, max_retries: int = 12, retry_delay: int = 5) -> Tuple[Union[ContainerClient, BlobClient], str]:
        """
        指定されたコンテナまたはblobのリースを取得します。

        Args:
            container_name (str): コンテナの名前
            blob_name (str, optional): blobの名前。指定しない場合はコンテナのリースを取得します。
            max_retries (int): 最大再試行回数
            retry_delay (int): 再試行間の待機時間（秒）

        Returns:
            Tuple[Union[ContainerClient, BlobClient], str]: リース取得に成功した場合は(クライアント, リースID)のタプル

        Raises:
            AzureError: リース取得に失敗した場合
        """
        client = self.get_client(container_name, blob_name)

        for attempt in range(max_retries):
            properties = client.get_blob_properties() if isinstance(client, BlobClient) else client.get_container_properties()
            if properties.lease.state != "leased":
                lease_client = BlobLeaseManager.acquire_lease(client)
                if lease_client:
                    # クライアントのメタデータから一意の識別子を取得
                    key = f"{properties['container']}/{properties['name']}" if isinstance(client, BlobClient) else properties['name']
                    self.__leases[key] = {
                        'lease_id': lease_client.id,
                        'lease_client': lease_client
                    }
                    return client, lease_client.id

            if attempt < max_retries - 1:
                resource_type = "Blob" if isinstance(client, BlobClient) else "Container"
                resource_name = f"'{blob_name}' in container '{container_name}'" if isinstance(client, BlobClient) else f"'{container_name}'"
                logging.info(f"{resource_type} {resource_name} has an active lease or lease acquisition failed. Attempt {attempt + 1}/{max_retries}. Waiting {retry_delay} seconds before retry.")
                time.sleep(retry_delay)

        resource_type = "Blob" if blob_name else "Container"
        resource_name = f"'{blob_name}' in container '{container_name}'" if blob_name else f"'{container_name}'"
        logging.error(f"Failed to acquire lease for {resource_type} {resource_name} after {max_retries} attempts.")
        raise AzureError(f"Failed to acquire lease for {resource_type} {resource_name}.")

    def release_lease(self, container_name: str, blob_name: str = None):
        """
        指定されたリースを解放します。

        Args:
            container_name (str): コンテナの名前
            lease_id (str): 解放するリースのID
            blob_name (str, optional): blobの名前。指定しない場合はコンテナのリースを解放します。
        """
        key = f"{container_name}/{blob_name}" if blob_name else container_name
        
        if key in self.__leases:
            lease_client = self.__leases.get(key)['lease_client']
            try:
                BlobLeaseManager.release(lease_client)
                del self.__leases[key]
                logging.info(f"Lease released for: {key}")
            except Exception as e:
                if "LeaseIdMismatchWithLeaseOperation" in str(e):
                    logging.debug(f"Lease '{key}' has already been released or acquired by another client.")
                else:
                    logging.error(f"An error occurred while releasing lease '{key}': {str(e)}")
        else:
            resource_type = "Blob" if blob_name else "Container"
            resource_name = f"{blob_name} in {container_name}" if blob_name else container_name
            logging.warning(f"No matching lease found for {resource_type} '{resource_name}'.")

    def release_all_leases(self):
        """
        すべてのリースを解放します。
        """
        errors = []
        for key, lease in list(self.__leases.items()):
            try:
                lease_client = lease.get('lease_client')
                BlobLeaseManager.release(lease_client)
                del self.__leases[key]
                logging.info(f"Lease '{key}' successfully released.")
            except Exception as e:
                if "LeaseIdMismatchWithLeaseOperation" in str(e):
                    logging.debug(f"Lease '{key}' has already been released or acquired by another client.")
                else:
                    logging.error(f"An error occurred while releasing lease '{key}': {str(e)}")

        if errors:
            for error in errors:
                logging.error(error)
            logging.warning(f"Errors occurred while releasing {len(errors)} lease(s). {len(self.__leases)} lease(s) remain unreleased.")
        else:
            logging.info("All leases successfully released.")
