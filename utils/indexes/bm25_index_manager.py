import io
import logging
import pickle
from typing import Dict, List, Optional, Tuple, Union

from janome.tokenizer import Tokenizer
from rank_bm25 import BM25Okapi

class BM25IndexManager:
    """
    BM25アルゴリズムを使用して文書のインデックスを管理するクラス。

    このクラスは文書の追加、削除、検索、およびインデックスの保存と読み込みの機能を提供します。
    日本語テキストに対応しており、Janomeトークナイザーを使用して文書をトークン化します。

    Attributes:
        __k1 (float): BM25アルゴリズムのk1パラメータ。
        __b (float): BM25アルゴリズムのbパラメータ。
        __tokenizer (Tokenizer): Janomeトークナイザーのインスタンス。
        __index (BM25Okapi): BM25Okapiインデックスのインスタンス。
        __docs (List[str]): インデックス化された文書のリスト。
        __deleted_flags (set): 削除されたドキュメントのインデックスを保持するセット。

    使用例:
        # インスタンスの作成
        bm25_manager = BM25IndexManager()

        # 文書の追加
        docs = ["これは最初の文書です。", "これは2番目の文書です。"]
        added_ids = bm25_manager.add(docs)

        # 単一の文書の追加
        single_doc_id = bm25_manager.add("これは3番目の文書です。")

        # 検索の実行
        query = "文書"
        results = bm25_manager.search(query, k=2)

        # 文書の削除
        bm25_manager.remove(0)

        # 複数の文書の削除
        bm25_manager.remove([1, 2])

        # インデックスの保存
        bm25_manager.save_to_file("index.pkl")

        # インデックスの読み込み
        loaded_manager = BM25IndexManager.load_from_file("index.pkl")

        # インデックスのエクスポート
        exported_data = bm25_manager.export()

        # バイトデータからのインデックスの読み込み
        imported_manager = BM25IndexManager.load_from_byte(exported_data)
        
        # インデックスの統計情報の取得
        stats = bm25_manager.stats
    """

    def __init__(self, index: Optional[BM25Okapi] = None, docs: Optional[List[str]] = None, 
             deleted_flags: Optional[set] = None, k1: float = 1.5, b: float = 0.75):
        """
        BM25IndexManagerのコンストラクタ。

        Args:
            index (Optional[BM25Okapi]): 既存のBM25Okapiインデックス。デフォルトはNone。
            docs (Optional[List[str]]): インデックス化する文書のリスト。デフォルトはNone。
            deleted_flags (Optional[set]): 削除されたドキュメントのインデックスを保持するセット。デフォルトはNone。
            k1 (float): BM25アルゴリズムのk1パラメータ。デフォルトは1.5。
            b (float): BM25アルゴリズムのbパラメータ。デフォルトは0.75。
        """
        self.logger = logging.getLogger(__name__)
        self.__k1 = k1
        self.__b = b
        self.__tokenizer = Tokenizer()
        
        if index is not None and (docs is None or deleted_flags is None):
            raise ValueError(f"If index is provided, docs and deleted_flags must also be provided. Got: index={index}, docs={docs}, deleted_flags={deleted_flags}")

        if index:
            if not isinstance(index, BM25Okapi):
                raise TypeError("index must be a BM25Okapi object")
            self.__index = index
            self.__docs = docs
            self.__deleted_flags = deleted_flags
            self.__update_index()
        else:
            self.__index = None
            self.__docs = []
            self.__deleted_flags = set()

        if docs:
            self.__update_index()

    def __tokenize(self, text: str) -> List[str]:
        """
        テキストをトークン化する内部メソッド。

        Args:
            text (str): トークン化するテキスト。

        Returns:
            List[str]: トークン化されたテキスト。
        """
        return [token.surface for token in self.__tokenizer.tokenize(text)]

    def add(self, documents: Union[str, List[str]]) -> List[int]:
        """
        インデックスに新しい文書を追加する。

        Args:
            documents (Union[str, List[str]]): 追加する文書または文書のリスト。

        Returns:
            List[int]: 追加された文書のID。

        Raises:
            ValueError: documentsが文字列またはリストでない場合。
        """
        if isinstance(documents, str):
            documents = [documents]
        elif not isinstance(documents, list) or not all(isinstance(doc, str) for doc in documents):
            raise ValueError('documents must be a string or a list of strings')

        start_id = len(self.__docs)
        self.__docs.extend(documents)
        new_ids = list(range(start_id, len(self.__docs)))

        # インデックスが空の場合でも新しいドキュメントを追加できるようにする
        if self.__index is None or len(self.__active_indices) == 0:
            self.__deleted_flags = set()  # 削除フラグをリセット
        
        self.__update_index()

        self.logger.info(f"{len(new_ids)} documents have been added.")
        return new_ids

    def remove(self, ids: Union[int, List[int]]):
        """
        インデックスから文書を削除する。

        Args:
            ids (Union[int, List[int]]): 削除する文書のIDまたはIDのリスト。

        Raises:
            ValueError: インデックスが初期化されていない場合。
        """
        if self.__index is None:
            raise ValueError("Index is not initialized. Please add documents first.")

        if isinstance(ids, int):
            ids = [ids]
        
        try:
            for id in ids:
                if 0 <= id < len(self.__docs):
                    self.__deleted_flags.add(id)

            self.__update_index()

            self.logger.info(f"{len(ids)} documents have been removed.")
        except Exception as e:
            self.logger.error(f"An error occurred while removing documents: {str(e)}")
            raise
        
    def unmark_deleted(self, ids: Union[int, List[int]]):
        """
        指定されたIDの文書の削除フラグを解除します。

        Args:
            ids (Union[int, List[int]]): 削除フラグを解除する文書のIDまたはIDのリスト。

        Raises:
            ValueError: インデックスが初期化されていない場合。
        """
        if self.__index is None:
            raise ValueError("Index is not initialized. Please add documents first.")

        if isinstance(ids, int):
            ids = [ids]
        
        try:
            for id in ids:
                if id in self.__deleted_flags:
                    self.__deleted_flags.remove(id)

            self.__update_index()

            self.logger.info(f"Deletion flags for {len(ids)} documents have been removed.")
        except Exception as e:
            self.logger.error(f"An error occurred while unmarking deleted documents: {str(e)}")
            raise

    def __update_index(self):
        """
        インデックスを更新する内部メソッド。
        削除されていないドキュメントのみを使用してインデックスを再構築します。
        """
        # 削除されていないドキュメントのみを抽出
        active_docs = [doc for i, doc in enumerate(self.__docs) if i not in self.__deleted_flags]
        
        # 各ドキュメントをトークン化
        tokenized_corpus = [self.__tokenize(doc) for doc in active_docs]
        
        # BM25Okapiインデックスを再構築
        self.__index = BM25Okapi(tokenized_corpus, k1=self.__k1, b=self.__b)
        
        # アクティブなドキュメントのインデックスを保持
        self.__active_indices = [i for i in range(len(self.__docs)) if i not in self.__deleted_flags]

    def search(self, query: str, k: int = 5) -> Tuple[List[int], List[float]]:
        if self.__index is None or len(self.__active_indices) == 0:
            self.logger.warning("Index is empty. No search results.")
            return [], []

        try:
            tokenized_query = self.__tokenize(query)
            doc_scores = self.__index.get_scores(tokenized_query)
            
            # スコアでソートし、上位k件を取得
            top_n = sorted(enumerate(doc_scores), key=lambda x: x[1], reverse=True)[:k]
            
            ids = [self.__active_indices[i] for i, _ in top_n]
            scores = [score for _, score in top_n]
            
            self.logger.info(f"Search results: {ids}, {scores}")
            return ids, scores
        except Exception as e:
            self.logger.error(f"An error occurred while executing the search query: {str(e)}")
            raise

    def save_to_file(self, file_path: str) -> None:
        """
        インデックスをファイルに保存する。

        Args:
            file_path (str): 保存先のファイルパス。

        Raises:
            ValueError: インデックスが初期化されていない場合。
        """
        if self.__index is None:
            raise ValueError("Index is not initialized. Please add documents first.")

        try:
            data = {
                'index': self.__index,
                'docs': self.__docs,
                'deleted_flags': self.__deleted_flags,
                'k1': self.__k1,
                'b': self.__b
            }
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
            self.logger.info(f"Index has been saved to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to save index to file: {str(e)}")
            raise

    @classmethod
    def load_from_file(cls, file_path: str) -> 'BM25IndexManager':
        logger = logging.getLogger(__name__)
        try:
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            instance = cls(index=data['index'], docs=data['docs'], deleted_flags=data['deleted_flags'], k1=data['k1'], b=data['b'])
            return instance
        except Exception as e:
            logger.error(f"Failed to load index from file: {str(e)}")
            raise

    @classmethod
    def load_from_byte(cls, byte_data: Union[bytes, io.BytesIO]) -> 'BM25IndexManager':
        """
        バイトデータからインデックスを読み込む。

        Args:
            byte_data (Union[bytes, io.BytesIO]): 読み込むバイトデータ。

        Returns:
            BM25IndexManager: 読み込まれたインデックスを持つBM25IndexManagerのインスタンス。

        Raises:
            Exception: データの読み込み中にエラーが発生した場合。
        """
        logger = logging.getLogger(__name__)
        try:
            if isinstance(byte_data, io.BytesIO):
                byte_data = byte_data.getvalue()
            
            data = pickle.loads(byte_data)
        
            instance = cls(index=data['index'], docs=data['docs'], deleted_flags=data['deleted_flags'], k1=data['k1'], b=data['b'])
            return instance
        except Exception as e:
            logger.error(f"Failed to load index from byte data: {str(e)}")
            raise
            
    def export(self) -> bytes:
        """
        インデックスをバイトデータとしてエクスポートする。

        Returns:
            bytes: シリアライズされたインデックスデータ。

        Raises:
            ValueError: インデックスが初期化されていない場合。
        """
        if self.__index is None:
            raise ValueError("Index is not initialized. Please add documents first.")

        try:
            data = {
                'index': self.__index,
                'docs': self.__docs,
                'deleted_flags': self.__deleted_flags,
                'k1': self.__k1,
                'b': self.__b
            }
            return pickle.dumps(data)
        except Exception as e:
            self.logger.error(f"An error occurred while exporting the index: {str(e)}")
            raise

    @property
    def stats(self) -> Dict[str, int]:
        """
        インデックスの統計情報を取得する。

        Returns:
            Dict[str, int]: 文書数、アクティブな文書数、語彙サイズを含む辞書。
        """
        if self.__index is None:
            return {
                "num_documents": 0,
                "active_documents": 0,
                "vocabulary_size": 0
            }
        return {
            "num_documents": len(self.__docs),
            "active_documents": len(self.__docs) - len(self.__deleted_flags),
            "vocabulary_size": len(self.__index.idf)
        }