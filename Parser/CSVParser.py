import io
from Parser import ENCODINGS, IParser, chunk_text
from typing import List, Dict, Any
import pandas as pd
import logging
import traceback

class CSVChunkParser(IParser):
    """
    CSVファイルをチャンクに変換するクラスです。
    パーサー用のインターフェイスであるIParserを継承しています。

    Attributes:
        __encoding (str): ファイルのエンコーディングです。
    """

    def __init__(self, encoding="UTF-8", separator="<br>"):
        """
        CSVChunkParserのインスタンスを初期化します。

        Args:
            encoding (str, optional): ファイルのエンコーディングです。デフォルトは "UTF-8" です。
        """
        self.__encoding = encoding
        self.__separator_charactor = separator

    def parse(self, file_content: bytes) -> List[Dict[str, Any]]:
        """
        CSVファイルをチャンクに変換するメソッドです。

        Args:
            file_content (bytes): ファイルの内容をバイト列で受け取ります。

        Returns:
            List[Dict[str, Any]]: 行番号と各列の値を含む辞書のリストを返します。
        """
        try:
            # 複数のencodingで読み込みを実施
            for encoding in ENCODINGS:
                try:
                    df = pd.read_csv(io.BytesIO(file_content), encoding=encoding)
                    self.__encoding = encoding
                    break
                except UnicodeDecodeError as e:
                    continue

            # データフレームの中からNaNを削除
            df = df.dropna(how='all')
            df = df.dropna(axis=1, how='all')        
            df = df.map(lambda x: x.replace('\n', '<br>') if isinstance(x, str) else x)
            
            csv_content = df.to_markdown()

            chunks = chunk_text(text=csv_content)
            chunks_without_empty = list(filter(None, chunks))
            
            page_with_chunk = [
                dict(page_number=i, texts=[chunk])
                for i, chunk in enumerate(chunks_without_empty)
            ]
            
            if not page_with_chunk:
                logging.critical("parsing failed")
                raise ValueError(f"Failed to parse document.")
            
        except Exception as e:
            logging.error(f"An error occurred while parsing the CSV file: {str(e)}")
            logging.debug(traceback.format_exc())
            return []

        return page_with_chunk
