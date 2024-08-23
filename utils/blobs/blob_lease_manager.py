import logging
from typing import Optional

from azure.storage.blob import ContainerClient, BlobClient, BlobLeaseClient
from azure.core.exceptions import AzureError

class BlobLeaseManager:
    """
    Azure Blob StorageのコンテナまたはBlobに対するリース操作を管理するクラスです。

    リースは複数のクライアントが同時に同じBlobリソースにアクセスすることを防ぐために使用されます。
    """
    
    @staticmethod
    def acquire_lease(client, lease_duration: int = 20) -> Optional[str]:
        """
        指定されたクライアント（コンテナまたはBlob）に対してリースを取得します。

        Args:
            client: リースを取得するクライアント（ContainerClientまたはBlobClient）
            lease_duration (int): リースの有効期間（秒）。デフォルトは60秒

        Returns:
            Optional[str]: 成功した場合はリースID、失敗した場合はNone

        Raises:
            ValueError: lease_durationが15〜60の範囲外の場合
            AzureError: リース取得に失敗した場合
        """
        try:
            if lease_duration != -1 and not (15 <= lease_duration <= 60):
                raise ValueError("lease_duration must be an integer between 15 and 60.")
            
            lease_client = client.acquire_lease(lease_duration=lease_duration)
            
            if isinstance(client, ContainerClient):
                logging.info(f"Acquired new lease (ID: {lease_client.id}) for container: {client.container_name}")
            elif isinstance(client, BlobClient):
                logging.info(f"Acquired new lease (ID: {lease_client.id}) for blob: {client.blob_name} in container: {client.container_name}")
            else:
                logging.info(f"Acquired new lease (ID: {lease_client.id}) for {client.url}")
            
            return lease_client
        except AzureError as e:
            if isinstance(client, ContainerClient):
                logging.error(f"Failed to acquire lease for container: {client.container_name}. Error: {str(e)}")
            elif isinstance(client, BlobClient):
                logging.error(f"Failed to acquire lease for blob: {client.blob_name} in container: {client.container_name}. Error: {str(e)}")
            else:
                logging.error(f"Failed to acquire lease for {client.url}. Error: {str(e)}")
            raise

    @staticmethod
    def release(lease_client: BlobLeaseClient):
        """
        指定されたリースを解放します。

        Args:
            client: リースを解放するクライアント（ContainerClientまたはBlobClient）
            lease_id (str): 解放するリースのID
        """
        try:
            lease_client.release()
            logging.info(f"Lease '{lease_client.id}' was successfully released.")
        except AzureError as e:
            if "BlobNotFound" in str(e):
                logging.debug(f"Blob associated with lease '{lease_client.id}' does not exist.")
            elif "LeaseIdMismatchWithLeaseOperation" in str(e):
                logging.debug(f"Lease '{lease_client.id}' has already been released or acquired by another client.")
            else:
                logging.error(f"Failed to release lease '{lease_client.id}': {str(e)}")