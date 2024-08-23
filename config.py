import os

# BLOBストレージの設定
BLOB_STORAGE_URI = os.environ.get("BlobStorageConnection__serviceUri")
DOC_CONTAINER_NAME = os.environ.get("DOC_CONTAINER_NAME")
DB_CONTAINER_NAME = os.environ.get("DB_CONTAINER_NAME")
CONTENT_CONTAINER_NAME = os.environ.get("CONTENT_CONTAINER_NAME")

# Document Intelligence APIの設定
DI_API_ENDPOINT = os.environ.get("DOCUMENT_INTELLIGENCE_API_ENDPOINT")
DI_API_KEY = os.environ.get("DOCUMENT_INTELLIGENCE_API_KEY")

# Azure OpenAIの設定
AOAI_API_KEY = os.environ.get("AOAI_API_KEY")
AOAI_API_ENDPOINT = os.environ.get("AOAI_API_ENDPOINT")
EMBEDDING_DEPLOYMENT_NAME = os.environ.get("EMMBEDING_DEPLOYMENT_NAME")
EMBEDDING_VERSION = os.environ.get("EMMBEDING_VERSION")

# GPT-4 Turboの設定
GPT4O_API_ENDPOINT = os.environ.get("GPT4O_AOAI_API_ENDPOINT")
GPT4O_API_KEY = os.environ.get("GPT4O_AOAI_API_KEY")
GPT4O_DEPLOYMENT_NAME = os.environ.get("GPT4O_DEPLOYMENT_NAME")
GPT4O_VERSION = os.environ.get("GPT4O_VERSION")

# その他の設定
QUEUE_STORAGE_URI = os.environ.get("QueueStorageConnection__serviceUri")
QUEUE_NAME = os.environ.get("QUEUE_NAME")
TEXT_TABLE_NAME = os.environ.get("TEXT_TABLE_NAME")
VECTOR_INDEX_FILE_NAME = os.environ.get("VECTOR_INDEX_FILE_NAME")
SEARCH_API_URL = os.environ.get("SEARCH_API_URL")
SEARCH_API_KEY = os.environ.get("SEARCH_API_KEY")

# Azure Functionsの設定（オプション）
AZURE_FUNCTIONS_STORAGE = os.environ.get("AzureWebJobsStorage", "UseDevelopmentStorage=true")