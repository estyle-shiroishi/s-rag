import logging
from typing import List

import numpy as np
from openai import AzureOpenAI, APIError, RateLimitError, APIConnectionError

class AzureEmbedder:
    """
    Azure OpenAIのテキスト埋め込みサービスを利用するためのクラス。

    このクラスは、Azure OpenAIのAPIを使用してテキストを数値ベクトルに変換します。
    単一のテキストや複数のテキストをバッチで処理することができます。
    出力は numpy.ndarray 型で、データ型は float32 です。

    Attributes:
        client (AzureOpenAI): Azure OpenAI APIクライアント
        deployment_name (str): 使用する埋め込みモデルのデプロイメント名

    使用例:
        embedder = AzureEmbedder(
            api_key="your_api_key",
            api_version="2023-05-15",
            azure_endpoint="https://your-resource-name.openai.azure.com/",
            deployment_name="your-embedding-deployment"
        )
        
        # 単一のテキストを埋め込む
        single_embedding = embedder.embed_single("Hello, World!")
        
        # 複数のテキストをバッチで埋め込む
        texts = ["Hello, World!", "埋め込みは有用です"]
        batch_embeddings = embedder.embed_batch(texts)
    """

    def __init__(self, api_key: str, api_version: str, azure_endpoint: str, deployment_name: str):
        """
        AzureEmbedderのコンストラクタ。

        Args:
            api_key (str): Azure OpenAI APIのキー
            api_version (str): 使用するAPIのバージョン
            azure_endpoint (str): Azure OpenAIのエンドポイントURL
            deployment_name (str): 使用する埋め込みモデルのデプロイメント名
        """
        self.client = AzureOpenAI(
            api_key=api_key,
            api_version=api_version,
            azure_endpoint=azure_endpoint,
            max_retries=5
        )
        self.deployment_name = deployment_name
        self.logger = logging.getLogger(__name__)

    def embed_single(self, text: str) -> np.ndarray:
        """
        単一のテキストを埋め込みベクトルに変換します。

        Args:
            text (str): 埋め込むテキスト

        Returns:
            np.ndarray: 埋め込みベクトル（float32型）

        Raises:
            ValueError: 入力テキストが空の場合
        """
        return np.array(self.__embed([text])[0], dtype=np.float32)

    def embed_batch(self, texts: List[str], batch_size: int = 100) -> np.ndarray:
        """
        複数のテキストをバッチで埋め込みベクトルに変換します。

        Args:
            texts (List[str]): 埋め込むテキストのリスト
            batch_size (int, optional): 一度に処理するテキストの数。デフォルトは100。

        Returns:
            np.ndarray: 埋め込みベクトルの2次元配列（float32型）

        Raises:
            ValueError: 入力テキストリストが空の場合
        """
        if not texts:
            raise ValueError("テキストリストが空です。")

        all_embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_embeddings = self.__embed(batch)
            all_embeddings.extend(batch_embeddings)

        return np.array(all_embeddings, dtype=np.float32)

    def __embed(self, texts: List[str]) -> List[List[float]]:
        """
        内部メソッド: テキストリストを埋め込みベクトルに変換します。

        Args:
            texts (List[str]): 埋め込むテキストのリスト

        Returns:
            List[List[float]]: 埋め込みベクトルのリスト

        Raises:
            ValueError: 入力テキストリストが空の場合
        """
        if not texts:
            raise ValueError("テキストリストが空です。")
        
        try:
            response = self.client.embeddings.create(
                input=texts,
                model=self.deployment_name
            )
            return [data.embedding for data in response.data]
        except RateLimitError as e:
            self.logger.error(f"Rate limit exceeded: {str(e)}")
            raise
        except APIConnectionError as e:
            self.logger.error(f"API connection error: {str(e)}")
            raise
        except APIError as e:
            self.logger.error(f"API error occurred: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            raise