from datetime import datetime, timezone

import pytest
from defusedxml.common import DefusedXmlException

from dl.parse import clean_text, news_id_for, parse_date, parse_feed

NOW = datetime(2026, 10, 31, 23, 0, tzinfo=timezone.utc)
META = dict(source_id="eltiempo_politica", medio="El Tiempo", seccion="Política",
            run_id="test", bronze_uri="s3://x/feed.xml", ingested_at=NOW)


def feed(items: str) -> bytes:
    return f'<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>t</title>{items}</channel></rss>'.encode()


def item(title="Titulo", link="https://www.eltiempo.com/politica/nota-3597559",
         pub="2026-10-30T16:30:50-05:00", desc="Desc") -> str:
    return (f"<item><title>{title}</title><link>{link}</link><guid>{link}</guid>"
            f"<pubDate>{pub}</pubDate><description>{desc}</description></item>")


def test_id_numerico_de_la_url():
    assert news_id_for("https://www.eltiempo.com/politica/x-3597559") == ("3597559", "url_numeric_id")


def test_id_de_respaldo_por_hash():
    news_id, strategy = news_id_for("https://www.eltiempo.com/politica/sin-numero")
    assert strategy == "link_hash" and news_id.startswith("h_")


def test_limpia_html_y_entidades():
    assert clean_text("<p>Hola&nbsp;<b>mundo</b></p><p>dos</p>") == "Hola mundo dos"


def test_mismo_instante_en_iso_y_rfc822():
    assert parse_date("2026-10-30T16:30:50-05:00") == parse_date("Fri, 30 Oct 2026 16:30:50 -0500")


def test_fecha_sin_zona_es_invalida():
    assert parse_date("2026-10-30T16:30:50") is None


def test_rechazos_con_motivo_en_orden():
    xml = feed(item(title="") + item(pub="ayer") + item(link="http://www.eltiempo.com/x-1234567") + item())
    valid, rejected = parse_feed(xml, **META)
    assert [r["rejection_reason"] for r in rejected] == ["titulo_vacio", "fecha_invalida", "enlace_invalido"]
    assert [r["item_position"] for r in rejected] == [1, 2, 3]
    assert len(valid) == 1


def test_dia_de_negocio_en_bogota():
    valid, _ = parse_feed(feed(item(pub="2026-10-31T01:30:00+00:00")), **META)
    assert valid[0]["pub_date"].isoformat() == "2026-10-30"


def test_rechaza_xxe():
    evil = (b'<?xml version="1.0"?><!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]>'
            b'<rss><channel><item><title>&x;</title></item></channel></rss>')
    with pytest.raises(DefusedXmlException):
        parse_feed(evil, **META)
