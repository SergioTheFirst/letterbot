def _extract_attachment_text(att: Attachment) -> str:
    name_lower = (att.filename or "").lower()
    content_type = (att.content_type or "").lower()
    
    logger.debug("Extracting from %s (type: %s, size: %d bytes)", 
                 att.filename, content_type, len(att.content))
    
    try:
        if name_lower.endswith(".pdf"):
            text = sanitize_text(extract_pdf_text(att.content, att.filename), max_len=5000)
            logger.info("PDF extraction: %d chars from %s", len(text), att.filename)
            return text
            
        if name_lower.endswith((".doc", ".docx")):
            text = sanitize_text(extract_docx_text(att.content, att.filename), max_len=5000)
            logger.info("DOC extraction: %d chars from %s", len(text), att.filename)
            return text
            
        if name_lower.endswith((".xls", ".xlsx")):
            text = sanitize_text(extract_excel_text(att.content, att.filename), max_len=5000)
            logger.info("Excel extraction: %d chars from %s", len(text), att.filename)
            return text
            
        if content_type.startswith("text") or name_lower.endswith((".txt", ".csv", ".log", ".md", ".json")):
            decoded = att.content.decode("utf-8", errors="ignore")
            text = sanitize_text(decoded, max_len=4000)
            logger.info("Text extraction: %d chars from %s", len(text), att.filename)
            return text
            
    except Exception as e:
        logger.error("Extraction failed for %s: %s", att.filename, e, exc_info=True)
    
    return ""
