from datetime import datetime
import json
import os
import logging
from typing import List, Dict, Tuple, Union
from utils.blobs.blob_manager import BlobManager
from utils.document_parser import DocumentParser
from utils.azure_embedder import AzureEmbedder
from utils.mapping.chunk_mapping_manager import ChunkMappingManager

class BlobDocumentProcessor:
    def __init__(self, blob_manager: BlobManager, document_parser: DocumentParser, embedding: AzureEmbedder, mapping_manager: ChunkMappingManager, db_container_name = 'db-container'):
        self.blob_manager = blob_manager
        self.container_manager = self.blob_manager.container_manager
        self.document_parser = document_parser
        self.embedding = embedding
        self.mapping_manager = mapping_manager
        self.db_container_name = db_container_name
        
    def __parse_document(self, blob_content: bytes, ext: str) -> Tuple[List[Dict[str, Union[str, int]]], List[Dict[str, Union[str, int]]], str]:
        """ドキュメントを解析し、全チャンク、ページごとのチャンク、全文を取得します。"""
        parsed_content = self.document_parser.parse_by_page(blob_content, ext)
        full_text = self.document_parser.parse_full_text(blob_content, ext)
        all_chunks = [{'text': text, 'page_number': page['page_number']} for page in parsed_content for text in page['texts']]
        page_chunks = [{'text': ''.join(page['texts']), 'page_number': page['page_number']} for page in parsed_content]
        return all_chunks, page_chunks, full_text
    
    def __get_file_extension(self, blob_name: str) -> str:
        """ファイル名から拡張子を取得します。"""
        _, ext = os.path.splitext(blob_name)
        return ext.lstrip('.').lower()
    
    def __process_document_to_chunks_and_fulltext(self, container_name: str, blob_name: str) -> Tuple[List[str], str]:
        """ドキュメントを処理し、チャンクと全文を取得します。"""
        blob_content = self.blob_manager.read(container_name, blob_name, as_byte=True)
        ext = self.__get_file_extension(blob_name)
        return self.__parse_document(blob_content, ext)

    def process_and_save_document(self, container_name: str, blob_name: str):
        """
        指定されたコンテナ内のドキュメントを処理し、データベースに保存します。
        既存のドキュメントが存在する場合は、新しいデータで上書きします。

        Args:
            container_name (str): ドキュメントが格納されているコンテナ名
            blob_name (str): 処理するドキュメントのブロブ名
        """
        try:
            # 既存のドキュメント、チャンク、ページチャンク、マッピングを確認
            existing_chunk_ids = self.mapping_manager.chunk_blob_mapping_manager.get_chunk_ids_by_blob(container_name, blob_name)
            
            if existing_chunk_ids:
                self.__delete_document_internal(container_name, blob_name)
                
            all_chunks, page_chunks, full_text = self.__process_document_to_chunks_and_fulltext(container_name, blob_name)
            
            # チャンクの保存
            self.chunk_info_list = self.mapping_manager.add(container_name, blob_name, [chunk['text'] for chunk in all_chunks])
            for (chunk_id, chunk), all_chunk in zip(self.chunk_info_list, all_chunks):
                self.blob_manager.upload(self.db_container_name, f'chunks/chunk_{chunk_id}.json', json.dumps({
                    'text': chunk,
                    'document_name': blob_name,
                    'page_number': all_chunk['page_number'],
                    'createdAt': datetime.now().isoformat()
                }))

            # ページチャンクの保存
            for page_chunk in page_chunks:
                self.blob_manager.upload(self.db_container_name, f'pages/{blob_name}_{page_chunk["page_number"]}.txt', page_chunk['text'])

            # 全文の保存
            self.blob_manager.upload(self.db_container_name, f'texts/{blob_name}.txt', full_text)

            # メタデータを更新
            safe_chunk_ids = json.dumps([str(id) for id, _ in self.chunk_info_list])
            self.blob_manager.add_metadata(container_name, blob_name, {"chunk_ids": safe_chunk_ids})
            logging.info(f"ドキュメント {blob_name} の処理が正常に完了しました。")

        except Exception as e:
            logging.error(f"ドキュメント {blob_name} の処理に失敗しました: {str(e)}")
            # エラーが発生した場合、部分的に作成されたデータを削除
            self.__delete_document_internal(container_name, blob_name)
            raise
        
    def delete_document(self, container_name: str, blob_name: str):
        """
        指定されたドキュメントをデータベースから削除します。

        Args:
            container_name (str): ドキュメントが格納されているコンテナ名
            blob_name (str): 削除するドキュメントのブロブ名
        """
        try:
            self.__delete_document_internal(container_name, blob_name)
        except Exception as e:
            logging.error(f"ドキュメント {blob_name} の削除中にエラーが発生しました: {str(e)}")
            raise

    def __delete_document_internal(self, container_name: str, blob_name: str):
        chunk_ids = self.mapping_manager.chunk_blob_mapping_manager.get_chunk_ids_by_blob(container_name, blob_name)
        for chunk_id in chunk_ids:
            chunk_blob_name = f'chunks/chunk_{chunk_id}.json'
            chunk_content = self.blob_manager.read(self.db_container_name, chunk_blob_name)
            chunk_data = json.loads(chunk_content)
            page_number = chunk_data.get('page_number')
            
            self.mapping_manager.remove(chunk_id)
            self.blob_manager.delete(self.db_container_name, chunk_blob_name)
            
            if page_number and self.blob_manager.blob_exist(self.db_container_name, f'pages/{blob_name}_{page_number}.txt'):
                self.blob_manager.delete(self.db_container_name, f'pages/{blob_name}_{page_number}.txt')

        # 全文の削除
        self.blob_manager.delete(self.db_container_name, f'texts/{blob_name}.txt')
        
        logging.info(f"ドキュメント {blob_name} が正常に削除されました。")