INSERT INTO analytics.dim_dates (dim_date, calendar_date, day_of_week, month, quarter, year)
SELECT
    TO_CHAR(dates, 'YYYYMMDD')::INT AS dim_date,
    dates AS calendar_date,
    TO_CHAR(dates, 'TMDay') AS day_of_week,
    EXTRACT(MONTH FROM dates)::INT AS month,
    EXTRACT(QUARTER FROM dates)::INT AS quarter,
    EXTRACT(YEAR FROM dates)::INT AS year
FROM generate_series('2026-01-01'::DATE, '2029-12-31'::DATE, '1 day'::INTERVAL) dates
ON CONFLICT (dim_date) DO NOTHING;