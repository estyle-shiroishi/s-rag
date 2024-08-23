from Parser import IParser, chunk_text
from typing import List, Dict, Any
from docx import Document
import io
import logging
import traceback

class DocxChunkParser(IParser):
    """
    Docxファイルをチャンクに変換するクラスです。
    パーサー用のインターフェイスであるIParserを継承しています。

    Attributes:
        __separator_charactor (str): チャンクを分割するためのセパレータ文字列です。
    """

    def __init__(self, separator="。"):
        """
        DocxChunkParserのインスタンスを初期化します。

        Args:
            separator (str, optional): チャンクを分割するためのセパレータ文字列です。デフォルトは "。" です。
        """
        self.__separator_charactor = separator

    def parse(self, file_content: bytes) -> List[Dict[str, Any]]:
        """
        Docxファイルをチャンクに変換するメソッドです。

        Args:
            file_content (bytes): ファイルの内容をバイト列で受け取ります。

        Returns:
            List[Dict[str, Any]]: ページ番号とテキストのリストを含む辞書のリストを返します。
        """
        logging.info(f"Start parsing docx file.")
        try: 
            doc = Document(io.BytesIO(file_content))

            text = ""
            for para in doc.paragraphs:
                text += para.text

            chunks = chunk_text(text=text)
            chunks_without_empty = list(filter(None, chunks))

            page_with_chunk = [
                dict(page_number=i, texts=[chunk])
                for i, chunk in enumerate(chunks_without_empty)
            ]
            
            if not page_with_chunk:
                logging.critical("parsing failed")
                raise ValueError(f"Failed to parse document.")
            
            logging.info(f"End parsing docx file.")
        except Exception as e:
            logging.error(f"An error occurred while parsing the Word file: {str(e)}")
            logging.debug(traceback.format_exc())
            return []

        return page_with_chunk
