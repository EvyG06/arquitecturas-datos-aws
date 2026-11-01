-- Cinco frases clave mas frecuentes por seccion, contando noticias distintas.
WITH total AS (
    SELECT seccion, phrase_norm,
           max_by(phrase_text, n_noticias) AS frase,
           sum(n_noticias) AS noticias
    FROM gold_key_phrase_daily
    GROUP BY seccion, phrase_norm
),
ranking AS (
    SELECT *, row_number() OVER (PARTITION BY seccion ORDER BY noticias DESC, frase) AS puesto
    FROM total
)
SELECT seccion, puesto, frase, noticias
FROM ranking
WHERE puesto <= 5
ORDER BY seccion, puesto
