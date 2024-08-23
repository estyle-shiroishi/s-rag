import azure.functions as func
import os
import json
import logging
import traceback
import requests

from utils.aoai import aoai_chatgpt
from utils.check_token import check_token

genie_bp = func.Blueprint()


@genie_bp.route(route="genie", methods=("POST",))
def genie(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('genie processed a request.')

    # requestのbodyを取得
    req_json = req.get_json()

    messages = req_json["messages"]

    # ファイルをアップロードする場合はここに処理を追加
    pass

    # contentの型の種類(str or list)に対応
    content = req_json["messages"][-1]["content"]
    user_input:str = ""
    if type(content) == str:
        user_input = content
    elif type(content) == list:
        for item in content:
            if "text" in item.keys():
                user_input = item["text"]
    logging.info("User input:" + user_input)

    # 何も入力がなかった場合
    if not user_input:
        logging.critical("何か入力してください。")
        res_json = {"choices":[{"message": {"content": "何か入力してください。"}}]}
        # ファイルアップロード用のblobsの追加 文字起こし後のblob名を格納
        res_json["blobs"] = []

        return func.HttpResponse(json.dumps(res_json))

    # 必要ならばここに検索クエリの生成ロジックを追加
    query:str = user_input

    # T-RAGを用いた検索の実施
    headers = {
        "Content-Type": "application/json",
        "x-functions-key": os.environ.get("SEARCH_API_KEY")
    }
    params = {
        "query": query,
        "max_results": 5
    }
    response = requests.get(
        url = os.environ.get("SEARCH_API_URL"),
        headers = headers,
        params = params
        )

    if response.status_code != 200:
        logging.critical(f"{response.status_code}, {response.text}")
        res_json = {"choices":[{"message": {"content": f"DBが動作しておりません。{response.status_code}, {response.text}"}}]}
        # ファイルアップロード用のblobsの追加 文字起こし後のblob名を格納
        res_json["blobs"] = []
        return func.HttpResponse(json.dumps(res_json))
    # json形式での読み込み
    search_results = json.loads(response.text)

    # 検索結果の整形 要調整
    max_tokens = 128000
    system_prompt = "ユーザーの質問に以下の情報を踏まえてMarkdown形式で回答してください。\n\n"
    system_prompt_tokens = check_token(system_prompt)
    
    db = ""
    total_tokens = system_prompt_tokens
    for res in search_results:
        new_entry = f"## [{res['source']}]\n{res['text']}\n\n"
        entry_tokens = check_token(new_entry)
        
        if total_tokens + entry_tokens > max_tokens:
            remaining_tokens = max_tokens - total_tokens
            partial_entry = new_entry[:remaining_tokens]
            db += partial_entry
            break
    
        db += new_entry
        total_tokens += entry_tokens

    system_content = f"{system_prompt}{db}"

    messages.append({
        "role": "system",
        "content": system_content
    })

        # 回答を生成
    try:
        response = aoai_chatgpt(messages)
    except Exception as e:
        tb = traceback.format_exc()
        response = f"ERROR: {e}, traceback: {tb}"
        logging.critical(response)

    # responseをUIで扱っている形に整形
    res_json = {"choices":[{"message": {"content": response}}]}
    # ファイルアップロード用のblobsの追加 文字起こし後のblob名を格納
    res_json["blobs"] = []
    return func.HttpResponse(json.dumps(res_json))

