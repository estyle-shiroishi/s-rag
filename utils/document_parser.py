import logging
from functools import partial
from typing import List, Dict, Any

from Parser import (
    TextParser,
    CSVParser,
    PDFParser,
    IMGParser,
    ExcelParser,
    WordParser,
    MailParser,
    PowerpointParser
)

class DocumentParser:
    """
    様々な形式のドキュメントを解析するためのクラス。

    このクラスは、テキスト、CSV、PDF、画像、Excel、Word、メール、PowerPointなど
    多様なファイル形式に対応したパーサーを提供します。APIエンドポイントとキーを
    使用して、特定のファイル形式（PDFや画像など）の解析を行います。

    Attributes:
        logger (logging.Logger): ロギング用のロガーオブジェクト。
        __parser_dict (dict): ファイル拡張子とそれに対応するパーサーのマッピング。

    使用方法:
        api_endpoint = "https://your-api-endpoint.com"
        api_key = "your-api-key"
        parser = DocumentParser(api_endpoint, api_key)

        # ファイルを読み込む
        with open("example.pdf", "rb") as file:
            content = file.read()

        # ページごとの解析結果を取得
        parsed_pages = parser.parse_by_page(content, "pdf")

        # 全テキストを取得
        full_text = parser.parse_full_text(content, "pdf")
    """

    def __init__(self, api_endpoint: str, api_key: str):
        """
        DocumentParserのコンストラクタ。

        Args:
            api_endpoint (str): 外部APIのエンドポイントURL。
            api_key (str): 外部APIの認証キー。
        """
        self.logger = logging.getLogger(__name__)
        self.__parser_dict = self.__create_parser_dict(api_endpoint, api_key)

    def __create_parser_dict(self, api_endpoint: str, api_key: str) -> Dict[str, Any]:
        """
        ファイル拡張子とパーサーのマッピングを作成する。

        Args:
            api_endpoint (str): 外部APIのエンドポイントURL。
            api_key (str): 外部APIの認証キー。

        Returns:
            Dict[str, Any]: ファイル拡張子とパーサーのマッピング。
        """
        # 各ファイル拡張子に対応するパーサーを定義
        extensions = {
            "image": ["png", "jpg", "jpeg"],
            "excel": ["xls", "xlsx"],
            "word": ["doc", "docx"],
            "powerpoint": ["ppt", "pptx"]
        }
        
        # 各ファイルタイプに対応するパーサークラスを定義
        parsers = {
            "image": partial(IMGParser.IMGChunkParser, api_endpoint=api_endpoint, api_key=api_key),
            "excel": ExcelParser.ExcelChunkParser,
            "word": WordParser.DocxChunkParser,
            "powerpoint": PowerpointParser.PPTXChunkParser
        }
        
        # 基本的なファイルタイプのパーサーを定義
        parser_dict = {
            "txt": TextParser.TextChunkParser,
            "csv": CSVParser.CSVChunkParser,
            "pdf": partial(PDFParser.PDFChunkParser, api_endpoint=api_endpoint, api_key=api_key),
            "msg": MailParser.MSGChunkParser,
        }

        # 拡張子とパーサーを関連付ける
        for parser_type, exts in extensions.items():
            parser_dict.update({ext: parsers[parser_type] for ext in exts})

        return parser_dict
    
    def parse_by_page(self, content: bytes, ext: str) -> List[Dict[str, Any]]:
        """
        ドキュメントをページごとに解析する。

        Args:
            content (bytes): 解析するドキュメントのバイナリデータ。
            ext (str): ドキュメントのファイル拡張子。

        Returns:
            List[Dict[str, Any]]: ページごとの解析結果。各ページは辞書形式で、
            'texts'キーにテキスト内容のリストが含まれる。

        Raises:
            ValueError: サポートされていないファイル拡張子が指定された場合。
        """
        # 指定された拡張子に対応するパーサーを使用してコンテンツを解析
        parser = self.__parser_dict.get(ext)
        if parser is None:
            self.logger.error(f"Unsupported file extension: {ext}")
            raise ValueError(f"Unsupported file extension: {ext}")
        
        parsed_content = parser().parse(content)
        return parsed_content
        
    def parse_full_text(self, content: bytes, ext: str) -> str:
        """
        ドキュメント全体のテキストを抽出する。

        Args:
            content (bytes): 解析するドキュメントのバイナリデータ。
            ext (str): ドキュメントのファイル拡張子。

        Returns:
            str: ドキュメント全体のテキスト。

        Raises:
            ValueError: サポートされていないファイル拡張子が指定された場合。
        """
        # ページごとに解析されたコンテンツから全テキストを抽出
        page_with_chunks = self.parse_by_page(content, ext)
        all_chunks = []
        for page in page_with_chunks:
            all_chunks.extend(page['texts'])
        return ''.join(all_chunks)