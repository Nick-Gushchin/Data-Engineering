SELECT *
FROM [dbo].[ProcessedData]
WHERE price IS NULL OR price <= 0;