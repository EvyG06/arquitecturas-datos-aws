-- Presencia diaria de una entidad y su cambio frente al dia anterior.
-- El calendario sale de gold_news_daily para que los dias sin menciones aparezcan con 0 y no desaparezcan.
WITH dias AS (
    SELECT DISTINCT pub_date FROM gold_news_daily
),
entidad AS (
    SELECT pub_date, sum(n_noticias) AS noticias
    FROM gold_entity_daily
    WHERE entity_norm = 'gobierno'
    GROUP BY pub_date
)
SELECT d.pub_date,
       coalesce(e.noticias, 0) AS noticias,
       coalesce(e.noticias, 0) - lag(coalesce(e.noticias, 0)) OVER (ORDER BY d.pub_date) AS cambio_vs_dia_anterior
FROM dias d
LEFT JOIN entidad e ON e.pub_date = d.pub_date
ORDER BY d.pub_date
