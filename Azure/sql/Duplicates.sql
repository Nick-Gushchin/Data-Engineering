SELECT id, COUNT(*) AS count
FROM [dbo].[ProcessedData]
GROUP BY id
HAVING COUNT(*) > 1;