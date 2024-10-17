SELECT *
FROM [dbo].[ProcessedData]
WHERE availability_365 IS NULL OR availability_365 < 0 OR availability_365 > 365;