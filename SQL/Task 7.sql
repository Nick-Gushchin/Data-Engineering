CREATE OR REPLACE FUNCTION fn_get_top_n_books_by_genre(
    p_genre_id INTEGER,
    p_top_n INTEGER
)
RETURNS TABLE (
    book_id INTEGER,
    title VARCHAR,
    total_revenue NUMERIC(10, 2)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        b.book_id,
        b.title,
        COALESCE(SUM(s.quantity * b.price), 0) AS total_revenue
    FROM
        Books b
    LEFT JOIN
        Sales s ON b.book_id = s.book_id
    WHERE
        b.genre_id = p_genre_id
    GROUP BY
        b.book_id, b.title
    ORDER BY
        total_revenue DESC
    LIMIT
        p_top_n;
END;
$$;

SELECT * FROM fn_get_top_n_books_by_genre(1, 5);