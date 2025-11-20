-- ICM (Interchain Messaging) sent metric
-- Parameters: chain_id, first_period, last_period, granularity

INSERT INTO metrics (chain_id, metric_name, granularity, period, value)
SELECT
    {chain_id} as chain_id,-- chains are indexed separately
    'icm_sent' as metric_name,-- all in one table
    '{granularity}' as granularity,--hour, day, week, month
    toStartOf{granularityCamelCase}(block_time) as period,--toStartOfHour, toStartOfDay, toStartOfWeek, toStartOfMonth
    count(*) as value
FROM raw_logs -- a raw table with all transaction receipts' logs flattened
WHERE chain_id = @chain_id
  AND block_time >= @first_period
  AND block_time < @last_period
  AND topic0 = unhex('2a211ad4a59ab9d003852404f9c57c690704ee755f3c79d2c2812ad32da99df8') -- sendCrossChainMessage
GROUP BY period -- group by toStartOfHour/Day/Week/Month
ORDER BY period;





