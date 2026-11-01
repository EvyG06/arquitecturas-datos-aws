-- Conciliacion: noticias distintas en Silver contra la suma de Gold, por fecha y seccion.
-- FULL OUTER JOIN para que una fila que exista en un lado y no en el otro tambien aparezca como descuadre.
WITH s AS (
    SELECT pub_date, seccion, count(DISTINCT news_id) AS silver
    FROM silver_news
    GROUP BY pub_date, seccion
),
g AS (
    SELECT pub_date, seccion, sum(n_noticias) AS gold
    FROM gold_news_daily
    GROUP BY pub_date, seccion
)
SELECT coalesce(s.pub_date, g.pub_date) AS pub_date,
       coalesce(s.seccion, g.seccion) AS seccion,
       s.silver,
       g.gold,
       CASE WHEN s.silver = g.gold THEN 'OK' ELSE 'DESCUADRE' END AS estado
FROM s
FULL OUTER JOIN g ON s.pub_date = g.pub_date AND s.seccion = g.seccion
ORDER BY 1, 2
