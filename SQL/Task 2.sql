WITH BooksWithThe AS (
    SELECT
        b.book_id,
        b.title,
        b.author_id,
        b.genre_id,
        b.published_date
    FROM
        Books b
    WHERE
        b.title ~* '\ythe\y'
)
SELECT
    bwt.title,
    a.name AS author_name,
    g.genre_name,
    bwt.published_date
FROM
    BooksWithThe bwt
JOIN
    Authors a ON bwt.author_id = a.author_id
JOIN
    Genres g ON bwt.genre_id = g.genre_id;