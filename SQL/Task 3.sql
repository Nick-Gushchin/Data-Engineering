SELECT
    b.title,
    g.genre_name,
    b.price,
    RANK() OVER (PARTITION BY b.genre_id ORDER BY b.price DESC) AS price_rank
FROM
    Books b
JOIN
    Genres g ON b.genre_id = g.genre_id
ORDER BY
    g.genre_name,
    price_rank;