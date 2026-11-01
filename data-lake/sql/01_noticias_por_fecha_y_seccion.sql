-- Noticias vigentes de una fecha y una seccion. Cambia la fecha y la seccion segun lo que quieras ver.
SELECT pub_date, seccion, news_id, published_at_utc, title, link
FROM silver_news
WHERE pub_date = DATE '2026-10-31'
  AND seccion = 'Política'
ORDER BY published_at_utc DESC
