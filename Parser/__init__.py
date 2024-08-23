from abc import ABC, abstractmethod
from typing import List, Dict, Any
from itertools import islice

# 文字エンコード
ENCODINGS = ['utf-8', 'shift_jis', 'euc_jp', 'iso2022_jp', 'cp932', 'utf-16', 'latin1']

# パーサーのインターフェース
class IParser(ABC):
    @abstractmethod
    def parse(self, file_content: bytes) -> List[Dict[str, Any]]:
        """
        ファイルの内容を解析し、辞書のリストとして返します。

        :param file_content: 解析するファイルの内容（バイト列）
        :return: 解析結果の辞書のリスト
        """
        pass


# パーサ用のユーティリティ関数
def chunk_text(text: str, chunk_size: int = 1024, overlap: int = 0, min_chunk_size: int = 1) -> list[str]:
    """
    テキストを指定されたサイズでチャンクに分割します。

    Args:
        text (str): 分割するテキスト
        chunk_size (int, optional): チャンクの最大文字数。デフォルトは1024です。
        overlap (int, optional): オーバーラップする文字数。デフォルトは0です。
        min_chunk_size (int, optional): チャンクの最小文字数。デフォルトは1です。

    Returns:
        list[str]: チャンクのリスト
    """
    if chunk_size <= overlap:
        raise ValueError("overlap must be smaller than chunk_size")

    text_length = len(text)
    if text_length <= chunk_size:
        return [text]

    chunks = []
    it = iter(text)
    while True:
        chunk = "".join(islice(it, chunk_size))
        if len(chunk) < min_chunk_size:
            break
        chunks.append(chunk)
        for _ in range(chunk_size - overlap):
            next(it, None)  # オーバーラップ分だけイテレータを進める

    return chunks
