-- Misma consulta que 07b, CON filtro sobre la columna de particion. Athena solo lee la carpeta de ese dia.
SELECT seccion, count(*) AS noticias, max(length(description)) AS desc_mas_larga
FROM silver_news
WHERE pub_date = DATE '2026-10-31'
GROUP BY seccion
ORDER BY seccion
