from Parser import ENCODINGS, IParser, chunk_text
from typing import List, Dict, Any
import logging
import traceback

class TextChunkParser(IParser):
    """
    テキストをチャンクに変換するクラスです。
    パーサー用のインターフェイスであるIParserを継承しています。

    Attributes:
        __separator_charactor (str): チャンクを分割するためのセパレータ文字列です。
        __encoding (str): ファイルのエンコーディングです。
    """

    def __init__(self, separator="。", encoding="UTF-8"):
        """
        TxtChunkParserのインスタンスを初期化します。

        Args:
            separator (str, optional): チャンクを分割するためのセパレータ文字列です。デフォルトは "。" です。
            encoding (str, optional): ファイルのエンコーディングです。デフォルトは "UTF-8" です。
        """
        self.__separator_charactor = separator
        self.__encoding = encoding

    def parse(self, file_content: bytes) -> List[Dict[str, Any]]:
        """
        テキストをチャンクに変換するメソッドです。

        Args:
            file_content (bytes): ファイルの内容をバイト列で受け取ります。

        Returns:
            List[Dict[str, Any]]: ページ番号とテキストのリストを含む辞書のリストを返します。
        """
        logging.info("Parsing text file.")
        try: 
            # 複数のencodingで読み込みを実施
            for encoding in ENCODINGS:
                try:
                    text = file_content.decode(encoding=encoding)
                    self.__encoding = encoding
                    break
                except UnicodeDecodeError as e:
                    continue

            logging.info(f"Text length: {len(text)}")
            chunks = chunk_text(text=text)
            chunks_without_empty = list(filter(None, chunks))
            
            page_with_chunk = [
                dict(page_number=i, texts=[chunk])
                for i, chunk in enumerate(chunks_without_empty)
            ]
            
            if not page_with_chunk:
                logging.critical("parsing failed")
                raise ValueError(f"Failed to parse document.")
            
        except Exception as e:
            logging.error(f"An error occurred while parsing the Text file: {str(e)}")
            logging.debug(traceback.format_exc())
            return []

        return page_with_chunk