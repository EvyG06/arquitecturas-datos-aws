from datetime import datetime, timezone

from dl.controlled import MARK, build_controlled
from dl.parse import parse_feed

META = dict(source_id="eltiempo_politica", medio="El Tiempo", seccion="Política", run_id="ctl",
            bronze_uri="s3://x/feed.xml", ingested_at=datetime(2026, 11, 1, 16, 0, tzinfo=timezone.utc))


def item(nid, title):
    link = f"https://www.eltiempo.com/politica/nota-{nid}"
    return (f"<item><title>{title}</title><link>{link}</link><guid>{link}</guid>"
            f"<pubDate>2026-11-01T09:00:00-05:00</pubDate><description>d</description></item>")


def test_copia_controlada_produce_cambio_duplicado_y_defectos():
    xml = (f'<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>t</title>'
           f'{item(3000001, "Uno")}{item(3000002, "Dos")}</channel></rss>').encode()
    body, summary = build_controlled(xml)
    valid, rejected = parse_feed(body, **META)
    assert summary["items_copia"] == 6
    assert [r["rejection_reason"] for r in rejected] == ["titulo_vacio", "fecha_invalida", "enlace_invalido"]
    assert [r["item_position"] for r in rejected] == [4, 5, 6]
    assert [r["news_id"] for r in valid] == ["3000001", "3000002", "3000002"]
    assert valid[0]["title"] == "Uno" + MARK
