from typing import List

def chunk_text(text: str, size: int = 2000, overlap: int = 250) -> List[str]:
    if not text:
        return []

    chunks = []
    start = 0
    length = len(text)

    while start < length:
        end = start + size
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        next_start = end - overlap
        if next_start <= start:
            next_start = end
        start = next_start
        if start < 0:
            start = 0

    return chunks
