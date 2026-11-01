import pyarrow as pa

from dl.catalog import glue_type, plan, tables


def test_tipos_de_glue():
    assert glue_type(pa.string()) == "string"
    assert glue_type(pa.int32()) == "int"
    assert glue_type(pa.float64()) == "double"
    assert glue_type(pa.timestamp("ms")) == "timestamp"
    assert glue_type(pa.list_(pa.string())) == "array<string>"


def test_todas_las_columnas_tienen_tipo_en_glue():
    names = []
    for _, _, name, schema in tables():
        names.append(name)
        for field in schema:
            glue_type(field.type)
    assert names == ["silver_news", "silver_news_entities", "silver_news_key_phrases",
                     "gold_news_daily", "gold_entity_daily", "gold_key_phrase_daily"]


def test_plan_de_particiones():
    current = {"2026-10-30": "s3://b/x/v=1/", "2026-10-31": "s3://b/x/v=1/", "2026-10-29": "s3://b/x/v=1/"}
    desired = {"2026-10-30": "s3://b/x/v=1/", "2026-10-31": "s3://b/x/v=2/", "2026-11-01": "s3://b/x/v=2/"}
    assert plan(current, desired) == {"crear": ["2026-11-01"], "actualizar": ["2026-10-31"], "eliminar": ["2026-10-29"]}
