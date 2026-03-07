from typing import Callable, List, Optional

from mailbot_v26.llm.chunker import chunk_text
from mailbot_v26.llm import prompts_ru


class LLMSummarizer:
    def __init__(self, llm_call: Optional[Callable[[str], str]]):
        self.llm_call = llm_call

    def summarize_email(self, text: str) -> str:
        if not text:
            return ""
        if not self.llm_call:
            return self._fallback(text)

        base = self._chunk_and_merge(
            text,
            prompts_ru.EMAIL_CHUNK,
            prompts_ru.EMAIL_MERGE,
        )
        if not base:
            return self._fallback(text)

        final_prompt = self._select_final_prompt(base, text)
        final = self._safe_call(final_prompt.format(summary=base))
        return final.strip() if final else self._fallback(text)

    def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
        if not text:
            return ""
        if not self.llm_call:
            return self._fallback(text)

        chunk_prompt = {
            "PDF": prompts_ru.ATTACHMENT_CHUNK_PDF,
            "EXCEL": prompts_ru.ATTACHMENT_CHUNK_EXCEL,
            "CONTRACT": prompts_ru.ATTACHMENT_CHUNK_CONTRACT,
        }.get(kind, prompts_ru.ATTACHMENT_CHUNK_GENERIC)

        merged = self._chunk_and_merge(text, chunk_prompt, prompts_ru.ATTACHMENT_MERGE)
        return merged if merged else self._fallback(text)

    def _chunk_and_merge(self, text: str, chunk_prompt: str, merge_prompt: str) -> str:
        chunks = chunk_text(text)
        if not chunks:
            return ""

        summaries: List[str] = []
        for chunk in chunks:
            out = self._safe_call(chunk_prompt.format(text=chunk))
            if out:
                cleaned = out.strip()
                if cleaned:
                    summaries.append(cleaned)

        if not summaries:
            return ""

        merged = self._safe_call(merge_prompt.format(summaries="\n".join(summaries)))
        return merged.strip() if merged else ""

    def _select_final_prompt(self, summary: str, raw_text: str) -> str:
        combined = f"{summary}\n{raw_text}".lower()
        for key, triggers in prompts_ru.TRIGGERS.items():
            if any(trigger.lower() in combined for trigger in triggers):
                return prompts_ru.FINAL_PROMPTS.get(key, prompts_ru.FINAL_PROMPTS["GENERIC"])
        return prompts_ru.FINAL_PROMPTS["GENERIC"]

    def _safe_call(self, prompt: str) -> str:
        if not self.llm_call:
            return ""
        try:
            result = self.llm_call(prompt)
            if isinstance(result, str):
                return result
            return ""
        except Exception:
            return ""

    def _fallback(self, text: str) -> str:
        trimmed = text.strip()[:600]
        if len(text.strip()) > 600:
            return trimmed + "..."
        return trimmed
