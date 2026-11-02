from dl.validate import evaluate

CONC = {"columns": ["pub_date", "seccion", "silver", "gold", "estado"],
        "rows": [["2026-11-01", "Mundo", "7", "7", "OK"], ["2026-11-02", "Mundo", "6", "5", "DESCUADRE"]]}
QUAL_OK = {"rows": [["news_id duplicado en vigentes", "0"], ["campos obligatorios vacios", "0"]]}
QUAL_BAD = {"rows": [["news_id duplicado en vigentes", "2"], ["campos obligatorios vacios", "0"]]}


def test_detecta_descuadre_y_fallas():
    result = evaluate(CONC, QUAL_BAD)
    assert not result["ok"]
    assert result["descuadres"][0]["seccion"] == "Mundo" and result["fallas"] == {"news_id duplicado en vigentes": 2}


def test_todo_ok():
    assert evaluate({"columns": CONC["columns"], "rows": CONC["rows"][:1]}, QUAL_OK)["ok"]
