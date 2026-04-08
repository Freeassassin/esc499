import re
text = "sum(l_extendedprice * (1 - l_discount))"

def apply_text_agg(text):
    out = ""
    idx = 0
    while True:
        match = re.search(r'(?i)\bsum\s*\(', text[idx:])
        if not match:
            out += text[idx:]
            break
        start_idx = idx + match.end()
        paren_count = 1
        curr = start_idx
        while curr < len(text) and paren_count > 0:
            if text[curr] == '(': paren_count += 1
            elif text[curr] == ')': paren_count -= 1
            curr += 1
        
        if paren_count == 0:
            end_idx = curr - 1
            inner = text[start_idx:end_idx]
            out += text[idx:idx+match.start()]
            out += f"COUNT(CAST(({inner}) AS VARCHAR))"
            idx = curr
        else:
            out += text[idx:start_idx]
            idx = start_idx
    return out

print(apply_text_agg(text))
