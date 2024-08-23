import tiktoken

def check_token(text: str) -> int:
    encoding = tiktoken.get_encoding("cl100k_base")
    token_integers = encoding.encode(text)
    return len(token_integers)