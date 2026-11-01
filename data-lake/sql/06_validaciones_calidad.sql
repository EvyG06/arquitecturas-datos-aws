-- Validaciones de calidad sobre el conjunto vigente. Toda fila debe tener fallas = 0.
SELECT 'news_id duplicado en vigentes' AS validacion,
       count(*) - count(DISTINCT news_id) AS fallas
FROM silver_news
UNION ALL
SELECT 'campos obligatorios vacios',
       count_if(news_id IS NULL OR title IS NULL OR title = '' OR link IS NULL OR published_at_utc IS NULL)
FROM silver_news
UNION ALL
SELECT 'enlace fuera de eltiempo.com o sin https',
       count_if(NOT regexp_like(link, '^https://([a-z0-9-]+\.)*eltiempo\.com/'))
FROM silver_news
UNION ALL
SELECT 'fecha de publicacion fuera de rango',
       count_if(published_at_utc > localtimestamp + INTERVAL '1' DAY OR year(published_at_utc) < 2000)
FROM silver_news
UNION ALL
SELECT 'entidades sin noticia vigente', count(*)
FROM silver_news_entities e
LEFT JOIN silver_news n ON n.news_id = e.news_id AND n.pub_date = e.pub_date
WHERE n.news_id IS NULL
UNION ALL
SELECT 'frases clave sin noticia vigente', count(*)
FROM silver_news_key_phrases k
LEFT JOIN silver_news n ON n.news_id = k.news_id AND n.pub_date = k.pub_date
WHERE n.news_id IS NULL
