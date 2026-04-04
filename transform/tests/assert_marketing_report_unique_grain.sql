-- Fails if the gold mart grain is broken (duplicate keys).
SELECT
    report_date,
    brand,
    category_l1,
    COUNT(*) AS row_count
FROM {{ ref('marketing_report') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
