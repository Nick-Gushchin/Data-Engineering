SELECT *
FROM [dbo].[ProcessedData]
WHERE latitude IS NULL OR longitude IS NULL
OR latitude < -90 OR latitude > 90
OR longitude < -180 OR longitude > 180;