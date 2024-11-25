WITH RankedData AS (
    SELECT
        *,
        NTILE(10) OVER(ORDER BY MEDIAN_COLUMN) as percentile_group,
        ROW_NUMBER() OVER(
            PARTITION BY NTILE(10) OVER(ORDER BY MEDIAN_COLUMN)
            ORDER BY RAND()
        ) as row_num
    FROM your_table
)
SELECT
    *,
    CASE percentile_group
        WHEN 1 THEN '0-10%'
        WHEN 2 THEN '10-20%'
        WHEN 3 THEN '20-30%'
        WHEN 4 THEN '30-40%'
        WHEN 5 THEN '40-50%'
        WHEN 6 THEN '50-60%'
        WHEN 7 THEN '60-70%'
        WHEN 8 THEN '70-80%'
        WHEN 9 THEN '80-90%'
        WHEN 10 THEN '90-100%'
    END as percentile_range
FROM RankedData
WHERE row_num <= 3
ORDER BY percentile_group, row_num
FETCH FIRST 20 ROWS ONLY