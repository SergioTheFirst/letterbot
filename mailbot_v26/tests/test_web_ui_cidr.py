import ipaddress

from mailbot_v26.web_observability.app import _ip_allowed


def test_cidr_match_blocks_outside() -> None:
    networks = [
        ipaddress.ip_network("192.168.0.0/16"),
        ipaddress.ip_network("10.0.0.0/8"),
    ]

    assert _ip_allowed("192.168.10.15", networks)
    assert _ip_allowed("10.10.10.10", networks)
    assert not _ip_allowed("203.0.113.10", networks)
    assert _ip_allowed("127.0.0.1", networks)
