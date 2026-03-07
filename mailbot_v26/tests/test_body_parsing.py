from email import message_from_bytes

import mailbot_v26.start as start


def test_extract_body_handles_unencoded_text():
    raw_email = b"""From: sender@example.com\r\nSubject: Test\r\n\r\nPlain text body\r\n"""
    email_obj = message_from_bytes(raw_email)

    body = start._extract_body(email_obj)

    assert body == "Plain text body"


def test_extract_body_handles_multipart_unencoded_text():
    raw_email = b"".join(
        [
            b"MIME-Version: 1.0\r\n",
            b"Content-Type: multipart/mixed; boundary=abc123\r\n\r\n",
            b"--abc123\r\n",
            b"Content-Type: text/plain; charset=utf-8\r\n\r\n",
            b"Body without transfer encoding\r\n",
            b"--abc123\r\n",
            b"Content-Type: application/octet-stream\r\n",
            b"Content-Disposition: attachment; filename=doc.bin\r\n\r\n",
            b"data\r\n",
            b"--abc123--\r\n",
        ]
    )

    email_obj = message_from_bytes(raw_email)

    body = start._extract_body(email_obj)

    assert body == "Body without transfer encoding"
