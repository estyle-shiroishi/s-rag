import io
from Parser import IParser, chunk_text
from typing import List, Dict, Any
import pandas as pd
import logging
import re
import traceback

class ExcelChunkParser(IParser):
    """
    Excelブックをチャンクに変換するクラスです。
    パーサー用のインターフェイスであるIParserを継承しています。

    Attributes:
        __encoding (str): ファイルのエンコーディングです。
    """

    def __init__(self, encoding="UTF-8", separator="¥n"):
        """
        ExcelChunkParserのコンストラクタです。

        Args:
            encoding (str, optional): ファイルのエンコーディングです。デフォルトは "UTF-8" です。
            separator (str, optional): チャンクの区切り文字です。デフォルトは "¥n" です。
        """
        self.__encoding = encoding
        self.__separator_charactor = separator

    def parse(self, file_content: bytes) -> List[Dict[str, Any]]:
        """
        Excelファイルをチャンクに変換するメソッドです。

        Args:
            file_content (bytes): ファイルの内容をバイト列で受け取ります。

        Returns:
            List[Dict[str, Any]]: 行番号と各列の値を含む辞書のリストを返します。
        """
        logging.info(f"Extracting texts from Excel.")
        try:
            df_dict = pd.read_excel(io.BytesIO(file_content), sheet_name=None)

            excel_content = ""
            for sheet_name, df in df_dict.items():
                df = df.fillna("")  # NaNを空文字に置換
                df = df.astype(str)  # 全ての値を文字列に変換
                excel_content += df.to_markdown(index=False, numalign="left", stralign="left") + self.__separator_charactor

            # 不要なMarkdown記号などを削除
            excel_content = excel_content.replace("|", "").replace("-", "").replace(":", "").strip()
            excel_content = excel_content.replace("  ", "") # 2つ以上の連続するスペースを削除
            excel_content = excel_content.replace("NaT", "") # NaTを削除
            # 0Unnamed 1Unnamedなどの列名を削除
            excel_content = re.sub(r"\d+Unnamed", "", excel_content)
            excel_content = excel_content.replace("Unnamed", "")

            chunks = chunk_text(text=excel_content)
            chunks_without_empty = list(filter(None, chunks))

            page_with_chunk = [
                dict(page_number=i, texts=[chunk])
                for i, chunk in enumerate(chunks_without_empty)
                ]
            if not page_with_chunk:
                logging.critical("parsing failed")
                raise ValueError(f"Failed to parse document.")
            
        except Exception as e:
            logging.error(f"An error occurred while parsing the Excel file: {str(e)}")
            logging.debug(traceback.format_exc())
            return []
        

        return page_with_chunk
