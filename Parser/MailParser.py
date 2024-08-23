from Parser import IParser, TextParser, CSVParser, PDFParser, IMGParser, ExcelParser, WordParser, PowerpointParser
from typing import List, Dict, Any
import extract_msg  # extract_msgライブラリをインポート
import os
import io
import logging
from functools import partial
import re
import traceback

DI_API_ENDPOINT = os.environ.get("DOCUMENT_INTELLIGENCE_API_ENDPOINT")
DI_API_KEY = os.environ.get("DOCUMENT_INTELLIGENCE_API_KEY")

class MSGChunkParser(IParser):
    """
    msgファイルをチャンクに変換するクラスです。
    パーサー用のインターフェイスであるIParserを継承しています。

    Attributes:
        __separator_charactor (str): チャンクを分割するためのセパレータ文字列です。
    """

    def __init__(self, separator="[。！？.?!\n]", max_chunk_size=1000, overlap=200):
        """
        MSGChunkParserのインスタンスを初期化します。

        Args:
            separator (str, optional): チャンクを分割するためのセパレータ文字列です。デフォルトは "[。！？.?!\n]" です。
            max_chunk_size (int, optional): チャンクの最大サイズです。デフォルトは1000です。
            overlap (int, optional): チャンク間のオーバーラップ文字数です。デフォルトは100です。
        """
        self.__separator_charactor = separator
        self.max_chunk_size = max_chunk_size
        self.overlap = overlap
        
        image_extensions = ["png", "jpg", "jpeg"]
        excel_extensions = ["xls", "xlsx"]
        word_extensions = ["doc", "docx"]
        powerpoint_extensions = ["ppt", "pptx"]

        self.__parser = {
            "txt": TextParser.TextChunkParser,
            "csv": CSVParser.CSVChunkParser,
            "pdf": partial(PDFParser.PDFChunkParser, api_endpoint=DI_API_ENDPOINT, api_key=DI_API_KEY),
            "msg": MSGChunkParser,
        }

        self.__parser.update({ext: partial(IMGParser.IMGChunkParser, api_endpoint=DI_API_ENDPOINT, api_key=DI_API_KEY) for ext in image_extensions})
        self.__parser.update({ext: ExcelParser.ExcelChunkParser for ext in excel_extensions})
        self.__parser.update({ext: WordParser.DocxChunkParser for ext in word_extensions})
        self.__parser.update({ext: PowerpointParser.PPTXChunkParser for ext in powerpoint_extensions})

    def parse(self, file_content: bytes) -> List[Dict[str, Any]]:
        """
        msgファイルをチャンクに変換するメソッドです。

        Args:
            file_content (bytes): ファイルの内容をバイト列で受け取ります。

        Returns:
            List[Dict[str, Any]]: ページ番号とテキストのリストを含む辞書のリストを返します。
        """
        
        try:
            msg = extract_msg.Message(io.BytesIO(file_content))

            subject = msg.subject if msg.subject else "No Subject"
            body = msg.body if msg.body else "No Body"
            sender = msg.sender if msg.sender else "Unknown Sender"
            to = msg.to if msg.to else "Unknown Recipient"
            cc = msg.cc if msg.cc else "No CC"
            date = msg.date if msg.date else "Unknown Date"

            email_text = f"""
            ### Email Information

            **Subject**: {subject}

            **Date**: {date}

            **Body**: {body}

            **Sender**: {sender}

            **To**: {to}

            **CC**: {cc}
            """

            chunks = self.__create_chunks(email_text)
            
            page_with_chunk = [
                dict(page_number=0, texts=chunks)
            ]
            
            for idx, attachment in enumerate(msg.attachments):
                file_name = attachment.name
                if not file_name:
                    continue
                
                file_ext = file_name.split(".")[-1].lower()
                file_content = attachment.data
                
                if file_ext in self.__parser:
                    parser = self.__parser[file_ext]()
                    content_text = parser.parse(file_content)
                    attachment_chunks = []
                    for pt in content_text:
                        attachment_chunks.extend(pt["texts"])
                    
                    attachment_text = f"#### [{file_name}]\n" + "\n".join(attachment_chunks)
                    attachment_chunks = self.__create_chunks(attachment_text)
                    
                    page_with_chunk.append(
                        dict(page_number=idx+1, texts=attachment_chunks)
                    )
                    
            if not page_with_chunk:
                logging.critical("parsing failed")
                raise ValueError(f"Failed to parse document.")
            
        except Exception as e:
            logging.error(f"An error occurred while parsing the MSG file: {str(e)}")
            logging.debug(traceback.format_exc())
            return []

        return page_with_chunk

    def __create_chunks(self, text: str) -> List[str]:
        """
        テキストを適切なサイズのチャンクに分割します。

            Args:
                text (str): 分割するテキスト

        Returns:
            List[str]: チャンクのリスト
        """
        chunks = []
        current_chunk = ""
        sentences = self.__text_separator(text)

        for sentence in sentences:
            if len(current_chunk) + len(sentence) <= self.max_chunk_size:
                current_chunk += sentence
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = sentence

            # オーバーラップを考慮
            while len(current_chunk) >= self.max_chunk_size - self.overlap:
                chunks.append(current_chunk[:self.max_chunk_size])
                current_chunk = current_chunk[self.max_chunk_size - self.overlap:]

        if current_chunk:
            chunks.append(current_chunk)

        return chunks
    
    def __text_separator(self, text: str) -> List[str]:
        """
        テキストを指定されたセパレータで分割します。

        Parameters:
            text (str): 分割するテキスト

        Returns:
            List[str]: 分割されたテキストのリスト
        """
        texts = re.split(rf"(?<={self.__separator_charactor})", text)  # 分割
        texts = [t.strip() for t in texts if t.strip()]   # 0文字のものを除外

        return texts
