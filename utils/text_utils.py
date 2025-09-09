import unicodedata

def limit_visible_chars(text: str, max_chars: int = 300) -> str:
    count = 0
    result = ''
    for char in text:
        if unicodedata.category(char)[0] == 'C':
            result += char
            continue
        count += 1
        result += char
        if count >= max_chars:
            break
    return result
