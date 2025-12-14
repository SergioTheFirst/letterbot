"""Static prompt guidance per domain."""

PROMPTS_BY_DOMAIN = {
    "BANK": (
        "Summarize banking messages with the action required, payment or transfer amounts, and any due dates or deadlines."
        " Highlight account references, authorization steps, and security instructions if present."
    ),
    "TAX": (
        "Extract required tax actions, filing or payment dates, and amounts owed or referenced."
        " Note specific forms, periods, or obligations that are called out."
    ),
    "LEGAL": (
        "Capture legal actions requested such as responses, submissions, or compliance steps, along with any deadlines."
        " Include referenced case numbers, courts, and monetary penalties or fees if stated."
    ),
    "CONTRACT": (
        "Identify contract actions like signing, approval, renewal, or revision with the parties involved."
        " Record effective dates, signature deadlines, and financial terms such as fees or penalties."
    ),
    "INVOICE": (
        "Summarize invoice details including amount, currency, payment instructions, and due date."
        " Mention invoice numbers, services or goods billed, and any discounts or penalties."
    ),
    "PRICE_LIST": (
        "Provide key pricing updates, including product or service names and the amounts or rates quoted."
        " Note validity dates, minimum order quantities, and any changes compared to prior pricing."
    ),
    "HR": (
        "Outline HR-related actions such as approvals, acknowledgments, or scheduling steps."
        " Capture dates for interviews, reviews, vacations, or policy effective periods, and include any compensation figures."
    ),
    "LOGISTICS": (
        "Summarize logistics updates with required actions, shipment or delivery dates, and tracking or order references."
        " Include quantities, freight costs, or storage fees when specified."
    ),
    "MARKETING": (
        "Highlight promotional actions like opting in, registering, or redeeming offers."
        " Capture discount amounts, promo codes, time windows, and any purchase conditions."
    ),
    "PERSONAL": (
        "Provide a brief summary of the personal message and any explicit requests or invitations."
        " Include dates, times, or locations for events and any commitments or gifts mentioned."
    ),
    "COURT": (
        "Extract court-related actions such as filings, appearances, or responses and their deadlines."
        " Note case identifiers, hearing dates, and any fines or fee amounts."
    ),
    "GOVERNMENT": (
        "Summarize government communications focusing on required actions, compliance steps, and deadlines."
        " Include referenced programs, permits, or regulations and any stated fees or payments."
    ),
    "CLIENT": (
        "Identify client requests or approvals needed, including deliverables and associated dates."
        " Capture budgets, quoted amounts, and meeting or milestone schedules."
    ),
    "SUPPLIER": (
        "Summarize supplier messages with requested actions such as confirmations, approvals, or adjustments."
        " Note delivery dates, quantities, prices, and any penalties or discounts."
    ),
    "IT": (
        "Outline IT-related actions like access changes, incident responses, or configuration updates."
        " Capture system names, outage or maintenance windows, and any cost or licensing notes."
    ),
    "DOMAIN_REGISTRAR": (
        "Summarize domain registration notices with renewal or verification actions and their deadlines."
        " Include domain names, expiration dates, fees, and any required confirmation steps."
    ),
    "FAMILY": (
        "Provide a concise summary of the family message and any commitments, favors, or plans requested."
        " Mention dates, times, and locations for gatherings or tasks."
    ),
    "INTERNAL": (
        "Outline internal communications with actions needed, responsible parties, and timelines."
        " Note budgets, resource requests, or schedule changes that affect the team."
    ),
    "UNKNOWN": (
        "Summarize the message with explicit actions requested, key amounts, and any dates mentioned."
        " Highlight identifiers, references, or instructions that clarify how to proceed."
    ),
}

__all__ = ["PROMPTS_BY_DOMAIN"]
