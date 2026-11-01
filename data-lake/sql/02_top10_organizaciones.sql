-- Diez organizaciones presentes en mas noticias distintas, sumando todas las fechas y secciones.
-- Sumar n_noticias es valido porque cada noticia tiene una sola fecha y una sola seccion primaria.
SELECT max_by(entity_text, n_noticias) AS organizacion,
       sum(n_noticias) AS noticias,
       sum(menciones) AS menciones
FROM gold_entity_daily
WHERE entity_type = 'organizacion'
GROUP BY entity_norm
ORDER BY noticias DESC, organizacion
LIMIT 10
