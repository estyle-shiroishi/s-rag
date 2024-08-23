import io
import logging
from typing import Dict, List, Literal, Optional, Tuple, Union

from numpy import ndarray
from voyager import Index, Space

class VoyagerIndexManager:
    """
    ベクトルインデックスを管理するクラス。
    
    このクラスは、ベクトルの追加、検索、削除、更新、およびインデックスの保存と読み込みを行います。
    
    使用例:
    vim = VoyagerIndexManager(ndims=128, space_type='cosine')

    # ベクトルの追加
    vectors = np.array([np.random.rand(128) for _ in range(10)])
    ids = vim.add(vectors)

    # ベクトルの削除
    vim.remove([ids[1], ids[2]])
    
    # ベクトルの検索
    query_vector = np.random.rand(128)
    result_ids, distances = vim.search(query_vector, k=3)

    # インデックスの保存と読み込み
    vim.save_to_file("index.bin")
    loaded_vim = VoyagerIndexManager.load_from_file("index.bin")

    # バイトデータからのインデックス読み込み
    byte_data = vim.export()
    loaded_vim = VoyagerIndexManager.load_from_byte(byte_data)

    # インデックスのエクスポート
    exported_bytes = vim.export()

    # 統計情報の取得
    stats = vim.stats 
    print(stats)
    """

    __space_map = {
        'euclidean': Space.Euclidean,
        'innerproduct': Space.InnerProduct,
        'cosine': Space.Cosine
    }
    
    def __init__(self, index: Optional[Index] = None, ndims: int = 3072, space_type: Literal['euclidean', 'innerproduct', 'cosine'] = 'cosine'):
        """
        VectorIndexManagerを初期化します。

        Args:
            index (Optional[Index]): 既存のIndexオブジェクト。Noneの場合は新しいインデックスを作成します。
            ndims (int): ベクトルの次元数。デフォルトは3072。
            space_type (Literal['euclidean', 'innerproduct', 'cosine']): 使用する空間タイプ。デフォルトは'cosine'。

        Raises:
            ValueError: 無効なspace_typeが指定された場合。
            TypeError: indexが指定されたが、Indexオブジェクトでない場合。
        """
        self.logger = logging.getLogger(__name__) 
        
        self.__space = self.__space_map.get(space_type)
        if not self.__space:
            raise ValueError('space_type must be one of: euclidean, innerproduct, or cosine')
        if index:
            if not isinstance(index, Index):
                raise TypeError("index must be an Index object")
            self.__index = index
        else:
            self.__ndims = ndims
            self.__index = Index(self.__space, num_dimensions=self.__ndims)

        
    def add(self, vectors: Union[ndarray, List[ndarray]]) -> List[int]:
        """
        ベクトルをインデックスに追加します。

        Args:
            vectors (Union[ndarray, List[ndarray]]): 追加する1次元または2次元のndarray、
                または1次元または2次元のndarrayのリスト。

        Returns:
            List[int]: 追加されたベクトルのID。

        Raises:
            ValueError: vectorsが適切な形状でない場合。
            Exception: ベクトルの追加中にエラーが発生した場合。
        """
        # 入力ベクトルの形状を正規化
        if isinstance(vectors, ndarray):
            if vectors.ndim == 1:
                vectors = vectors.reshape(1, -1)
            elif vectors.ndim != 2:
                raise ValueError("Input array must be 1D or 2D")
        elif isinstance(vectors, list):
            vectors = [v.reshape(1, -1) if v.ndim == 1 else v for v in vectors]
            if not all(isinstance(v, ndarray) and v.ndim == 2 for v in vectors):
                raise ValueError('vectors must be a 1D or 2D numpy array, or a list of such arrays')
        else:
            raise ValueError('vectors must be a numpy array or a list of numpy arrays')

        try:
            ids = self.__index.add_items(vectors=vectors)
            self.logger.info(f"Added {len(ids)} records to the index.")
            return ids
        except Exception as e:
            self.logger.error(f"Error occurred while adding vectors to the index: {str(e)}")
            raise
    
    def remove(self, ids: Union[int, List[int]]):
        """
        指定されたIDのベクトルを削除します。

        Args:
            ids (Union[int, List[int]]): 削除するベクトルのIDまたはIDのリスト。

        Raises:
            Exception: 削除中にエラーが発生した場合。
        """
        if isinstance(ids, int):
            ids = [ids]
        
        for id in ids:
            try:
                self.__index.mark_deleted(id)
            except Exception as e:
                self.logger.error(f"Failed to delete vector with ID {id}: {str(e)}")
                raise

        self.logger.info(f"Removed {len(ids)} vectors from the index.")
        
    def unmark_deleted(self, ids: Union[int, List[int]]):
        """
        指定されたIDのベクトルの削除フラグを解除します。

        Args:
            ids (Union[int, List[int]]): 削除フラグを解除するベクトルのIDまたはIDのリスト。

        Raises:
            Exception: 削除フラグの解除中にエラーが発生した場合。
        """
        if isinstance(ids, int):
            ids = [ids]
        
        for id in ids:
            try:
                self.__index.unmark_deleted(id)
            except Exception as e:
                self.logger.error(f"Failed to unmark deleted for vector with ID {id}: {str(e)}")
                raise

        self.logger.info(f"Unmarked {len(ids)} vectors as deleted in the index.")
        
    def search(self, query_vector: ndarray, k: int = 5) -> Tuple[List[int], List[float]]:
        """
        クエリベクトルに最も近い k 個のベクトルを検索します。

        Args:
            query_vector (ndarray): 検索クエリベクトル。
            k (int): 返す結果の数。

        Returns:
            Tuple[List[int], List[float]]: (IDのリスト, 距離のリスト)

        Raises:
            Exception: 検索中にエラーが発生した場合。
        """
        try:
            ids, distances = self.__index.query(query_vector, k=k)
            self.logger.info(f"Search results: IDs={ids}, distances={distances}")
            return ids, distances
        except Exception as e:
            self.logger.error(f"Error occurred during search query: {str(e)}")
            raise
        
    @classmethod
    def load_from_file(cls, file_path: str) -> 'VoyagerIndexManager':
        """
        ファイルからインデックスを読み込み、新しいVectorIndexManagerインスタンスを作成します。

        Args:
            file_path (str): 読み込むファイルのパス。

        Returns:
            VectorIndexManager: 読み込んだインデックスを持つ新しいインスタンス。

        Raises:
            Exception: ファイルの読み込み中にエラーが発生した場合。
        """
        try:
            loaded_index = Index.load(file_path)
            return cls(index=loaded_index)
        except Exception as e:
            logging.error(f"Failed to load index from file: {str(e)}")
            raise
        
    @classmethod
    def load_from_byte(cls, byte_data: Union[bytes, io.BytesIO]) -> 'VoyagerIndexManager':
        """
        バイトデータからインデックスを読み込み、新しいVectorIndexManagerインスタンスを作成します。

        Args:
            byte_data (Union[bytes, io.BytesIO]): 読み込むバイトデータ。

        Returns:
            VectorIndexManager: 読み込んだインデックスを持つ新しいインスタンス。

        Raises:
            Exception: バイトデータの読み込み中にエラーが発生した場合。
        """
        try:
            # バイトデータをBytesIOオブジェクトに変換（必要な場合）
            if isinstance(byte_data, bytes):
                byte_data = io.BytesIO(byte_data)
            loaded_index = Index.load(byte_data)
            return cls(index=loaded_index)
        except Exception as e:
            logging.error(f"Failed to load index from byte data: {str(e)}")
            raise
        
    def save_to_file(self, file_path: str) -> None:
        """
        インデックスをファイルに保存します。

        Args:
            file_path (str): 保存先のファイルパス。

        Raises:
            Exception: ファイルの保存中にエラーが発生した場合。
        """
        try:
            self.__index.save(file_path)
            self.logger.info(f"Index saved to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to save index to file: {str(e)}")
            raise

    
    def export(self) -> io.BytesIO:
        """
        インデックスをバイトストリームとしてエクスポートします。

        Returns:
            io.BytesIO: インデックスのバイトストリーム。

        Raises:
            Exception: エクスポート中にエラーが発生した場合。
        """
        try:
            index_bytes = io.BytesIO()
            self.__index.save(index_bytes)
            index_bytes.seek(0)  # ストリームの位置を先頭に戻す
            return index_bytes
        except Exception as e:
            self.logger.error(f"Failed to export index: {str(e)}")
            raise

    @property
    def stats(self) -> Dict[str, Union[int, str]]:
        """
        インデックスの統計情報を取得します。

        Returns:
            Dict[str, Union[int, str]]: インデックスの統計情報を含む辞書。
        """
        return {
            "num_vectors": len(self.__index),
            "num_dimensions": self.__ndims,
            "space_type": self.__space
        }