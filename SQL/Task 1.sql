WITH AuthorBookCount AS (
    SELECT
        author_id,
        COUNT(*) AS total_books
    FROM
        Books
    GROUP BY
        author_id
)
SELECT
    a.name,
    abc.total_books
FROM
    AuthorBookCount abc
JOIN
    Authors a ON abc.author_id = a.author_id
WHERE
    abc.total_books > 3;