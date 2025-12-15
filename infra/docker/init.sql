CREATE TABLE IF NOT EXISTS ship_counts (
    time        TIMESTAMPTZ       NOT NULL,
    hex_id      TEXT              NOT NULL,
    count       INTEGER           NOT NULL,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    avg_speed   DOUBLE PRECISION
);

SELECT create_hypertable('ship_counts', 'time', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS unique_ship_counts_idx ON ship_counts (time, hex_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS minute_hex_volume
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) as minute,
    hex_id,
    last(count, time) as final_count
FROM ship_counts
GROUP BY 1, 2;

CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_baseline
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', minute) as hour,
    AVG(total_ships) as avg_hourly_volume
FROM (
    SELECT minute, SUM(final_count) as total_ships
    FROM minute_hex_volume
    GROUP BY 1
) as minute_totals
GROUP BY 1;

SELECT add_continuous_aggregate_policy('minute_hex_volume',
    start_offset => INTERVAL '1 hour',
    end_offset   => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '10 minutes'
);

SELECT add_continuous_aggregate_policy('hourly_baseline',
    start_offset => INTERVAL '3 hour',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);