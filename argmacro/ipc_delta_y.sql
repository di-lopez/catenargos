CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.' || :schema || '.ipc_delta_y') AS
SELECT
    date,
    ROUND(((value / lag(value, 1) OVER (ORDER BY date)) - 1) * 100, 1)  AS value
FROM IDENTIFIER(:catalog || '.' || :schema || '.ipc')
QUALIFY LAG(value, 1) OVER (ORDER BY date) IS NOT NULL
ORDER BY date