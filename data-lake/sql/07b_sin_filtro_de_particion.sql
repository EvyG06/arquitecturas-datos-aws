-- Misma consulta que 07a, SIN filtro de particion. Athena lee todas las fechas.
SELECT seccion, count(*) AS noticias, max(length(description)) AS desc_mas_larga
FROM silver_news
GROUP BY seccion
ORDER BY seccion
