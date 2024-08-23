import os
import logging
from openai import AzureOpenAI

from config import GPT4O_API_KEY, GPT4O_API_ENDPOINT, GPT4O_DEPLOYMENT_NAME, GPT4O_VERSION

# AOAIを経由してChatGPTを利用する
def aoai_chatgpt(prompt, model=GPT4O_DEPLOYMENT_NAME) -> str:
    """
    Azure OpenAIのGPT-4を利用して画像やテキストを処理します。

    Parameters:
        prompt (str or bytes): プロンプト (テキストまたは画像データ)
        model (str): 利用するモデル名

    Returns:
        str: GPT-4による処理結果
    """
    
    gpt4o_client = AzureOpenAI(
        api_key=GPT4O_API_KEY,  
        api_version=GPT4O_VERSION,
        azure_endpoint=GPT4O_API_ENDPOINT,
        max_retries=5
    )
    response = gpt4o_client.chat.completions.create(
        model=model,
        messages=prompt,
        temperature=0
    )
    return response.choices[0].message.content
