from Parser import IParser
from typing import List, Dict, Any
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import logging
import os
import re
import pandas as pd
import traceback

class PDFChunkParser(IParser):
    """
    テキストをチャンクに変換するクラスです。
    パーサー用のインターフェイスであるIParserを継承しています。

    Attributes:
        __separator_charactor (str): チャンクを分割するためのセパレータ文字列です。
        __encoding (str): ファイルのエンコーディングです。
    """

    def __init__(self, api_endpoint: str, api_key: str, separator="[。！？.?!\n]", encoding="UTF-8", max_chunk_size=1000, overlap=200):
        """
        TxtChunkParserのインスタンスを初期化します。

        Args:
            separator (str, optional): チャンクを分割するためのセパレータ文字列です。デフォルトは "[。！？.?!\n]" です。
            encoding (str, optional): ファイルのエンコーディングです。デフォルトは "UTF-8" です。
            max_chunk_size (int, optional): チャンクの最大サイズです。デフォルトは1000です。
            overlap (int, optional): チャンク間のオーバーラップ文字数です。デフォルトは100です。
        """
        self.__separator_charactor = separator
        self.__encoding = encoding
        self.__document_analysis_client = DocumentAnalysisClient(
            endpoint=api_endpoint, credential=AzureKeyCredential(api_key)
        )
        self.__custom_dict = {
            ":selected:": "",
            ":unselected:": "",
            "\t": " " * 4,
            " ": "",
        }
        self.max_chunk_size = max_chunk_size
        self.overlap = overlap

    def parse(self, file_content: bytes) -> List[Dict[str, Any]]:
        """
        PDFをチャンクに変換するメソッドです。

        Args:
            file_content (bytes): ファイルの内容をバイト列で受け取ります。

        Returns:
            List[Dict[str, Any]]: ページ番号とテキストのリストを含む辞書のリストを返します。
        """
        # PDFファイルをDocument Intelligenceで解析する
        # 返却されるデータ形式：
        # [
        #     {"page_number": 1, "texts": ["ページ1のテキスト1", "ページ1のテキスト2", ...]},
        #     {"page_number": 2, "texts": ["ページ2のテキスト1", "ページ2のテキスト2", ...]},
        #     # ... (以下、ページ数に応じて続く)
        # ]
        try:
            page_with_text = self.__document_intelligence(file_content)

            page_with_chunks = []
            for pt in page_with_text:
                chunked_texts = self.__create_chunks(pt['texts'])
                page_with_chunks.append(
                    dict(page_number=pt["page_number"], texts=chunked_texts)
                )
            
            if not page_with_chunks:
                logging.critical("parsing failed")
                raise ValueError(f"Failed to parse document.")
            
            logging.info("Texts were chunked.")
        except Exception as e:
            logging.error(f"An error occurred while parsing the PDF file: {str(e)}")
            logging.debug(traceback.format_exc())
            return []
        
        return page_with_chunks
    
    def __document_intelligence(self, file_content: bytes) -> List[Dict[str, Any]]:
        """
        PDFファイルのテキストをcontent単位で出力する。
        返却結果は下記の通りとなる。

        例:
        [
            {"page_number": 1, "texts": ["ページ1のテキスト1", "ページ1のテキスト2", ...]},
            {"page_number": 2, "texts": ["ページ2のテキスト1", "ページ2のテキスト2", ...]},
            # ... (以下、ページ数に応じて続く)
        ]
        """
        
        # Azure Cognitive ServicesのDocument Analysisを使用
        # documentを解析
        logging.info(f"Document analysis started.")
        poller = self.__document_analysis_client.begin_analyze_document(
            "prebuilt-document", file_content
        )
        result = poller.result()
        logging.info(f"Document analysis completed.")

        # paragraphごとにtextとpageを取得
        page_with_text = self.__result2text(result)
        
        page_dict = []
        for page_num, text in enumerate(page_with_text):
            page_dict.append(dict(texts=text, page_number=page_num + 1))
            logging.info(f"chunk: {text}")
        # print(text)
        logging.info(f"Document analysis completed.")
        return page_dict
    
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
    
    def __table2md(self, table) -> str:
        data = table.to_dict()
        # 最大の行数と列数を定義
        max_rows, max_cols = data["row_count"], data["column_count"]

        # リスト（テーブル）を初期化
        table = [["" for _ in range(max_cols)] for __ in range(max_rows)]

        # セルの内容を適切な位置に配置
        for cell in data['cells']:
            # セルの開始行と列を取得
            row, col = cell['row_index'], cell['column_index']
            # セルの内容をテーブルに配置
            table[row][col] = cell['content']

        # table
        df = pd.DataFrame(table[1:], columns=table[0])
        df = df.map(lambda x: x.replace('\n', '<br>') if isinstance(x, str) else x)
        return df.to_markdown()
    
    def __result2text(self, result) -> str:
        ocr_text = ["" for i in range(len(result.pages))]
        for page_num, page in enumerate(result.pages):
            tables_on_page = [table for table in result.tables if
                            table.bounding_regions[0].page_number == page_num + 1]

            # mark all positions of the table spans in the page
            page_offset = page.spans[0].offset
            page_length = page.spans[0].length
            table_chars = [-1] * page_length
            for table_id, table in enumerate(tables_on_page):
                for span in table.spans:
                    # replace all table spans with "table_id" in table_chars array
                    for i in range(span.length):
                        idx = span.offset - page_offset + i
                        if idx >= 0 and idx < page_length:
                            table_chars[idx] = table_id
            # build page text by replacing charcters in table spans with table html
            page_text = ""
            added_tables = set()
            for idx, table_id in enumerate(table_chars):
                if table_id == -1:
                    page_text += result.content[page_offset + idx]
                elif not table_id in added_tables:
                    page_text += "\n" + self.__table2md(tables_on_page[table_id]) + "\n"
                    added_tables.add(table_id)

            # page_text += " "
            # page_map.append((page_num, page_text))
            ocr_text[page_num] += page_text
        return ocr_text
    
    def __rechunk(self, chunks: List[str]) -> List[str]:
        """。を使ってチャンクを改善する"""
        separated_chunks = self.__merge_until_separator(chunks)  # 。が出てくるまでテキストを結合する
        dicted_chunks = [self.__use_dict(x) for x in separated_chunks]  # カスタム辞書を使用
        
        return dicted_chunks
    
    def __merge_until_separator(self, input_list: List[str]) -> List[str]:
        """
        指定されたセパレータまで文字列をマージするメソッドです。

        Args:
            input_list (List[str]): マージする文字列のリスト

        Returns:
            List[str]: マージされた文字列のリスト
        """
        output_list = []
        current_string = ""
        logging.info(f"Merge until separator started.")

        for string in input_list:
            if current_string:  # 文字があれば末尾にスペースを追加
                current_string += " " * 2 + os.linesep  # Markdown形式で改行
            current_string += string  # 文字を追加

            if string.endswith(self.__separator_charactor):  # 末尾が指定した文字だった場合リストに追加し、次へ進む
                output_list.append(current_string)
                current_string = ""

        logging.info(f"Merge until separator completed.")
        return output_list


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


    def __use_dict(self, text: str) -> str:
        """
        辞書を使用してテキストを変換します。

        Args:
            text (str): 変換するテキスト

        Returns:
            str: 変換後のテキスト
        """
        for old, new in self.__custom_dict.items():
            text = text.replace(old, new).strip()
        
        return text
