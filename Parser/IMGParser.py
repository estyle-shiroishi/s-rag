from Parser import IParser
from typing import List, Dict, Any

from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import logging
import os
import re
import traceback

class IMGChunkParser(IParser):

    def __init__(self, api_endpoint: str, api_key: str, separator="。", encoding="UTF-8"):
        """
        クラスの初期化メソッドです。

        Parameters:
            api_endpoint (str): Document IntelligenceのエンドポイントのURL
            api_key (str): Document IntelligenceAPIのキー
            separator (str, optional): 文章の区切り文字。デフォルトは "。"
            encoding (str, optional): 文字エンコーディング。デフォルトは "UTF-8"

        Returns:
            None
        """
        self.__separator_charactor = separator
        self.__encoding = encoding
        self.__document_analysis_client = DocumentAnalysisClient(
            endpoint=api_endpoint, credential=AzureKeyCredential(api_key)
        )

    def parse(self, file_content: bytes) -> List[Dict[str, Any]]:
        """
        画像の内容をDocument Intelligenceで解析し、内容からチャンクを抽出します。

        Parameters:
            file_content (bytes): 解析するファイルのコンテンツ

        Returns:
            List[Dict[str, Any]]: ページごとにチャンクが含まれる辞書のリスト
        """

        logging.info(f"Extracting texts from image.")
        try: 
            poller = self.__document_analysis_client.begin_analyze_document(
                api_version="2023-07-31", model_id="prebuilt-layout", document=file_content
            )
        
            result = poller.result()
            page_with_text = []
            for idx, page in enumerate(result.pages):
                texts = []
                for line in page.lines:
                    line_content = line.content
                    if line_content.__len__() <= 10: # 10文字以下の行は無視
                        continue
                    texts.append(line.content)
                page_with_text.append(dict(page_number=idx, texts=texts))
            
            if not page_with_text:
                logging.critical("parsing failed")
                raise ValueError(f"Failed to parse document.")

            logging.info(f"Texts were extracted from image.")
        except Exception as e:
            logging.error(f"An error occurred while parsing the IMG file: {str(e)}")
            logging.debug(traceback.format_exc())
            return []

        return page_with_text
    