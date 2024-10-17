SELECT *
FROM [dbo].[ProcessedData]
WHERE minimum_nights IS NULL OR minimum_nights <= 0;