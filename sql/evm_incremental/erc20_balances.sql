-- ERC-20 Balance Tracking - Block Range Processing
-- Stores: chain_id, wallet, token, block_number, balance
-- Balance is the wallet's balance AFTER this block executes
-- Uses ReplacingMergeTree for idempotency

CREATE TABLE IF NOT EXISTS erc20_balances (
    chain_id UInt32,
    wallet FixedString(20),
    token FixedString(20),
    block_number UInt32,
    balance UInt256,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (chain_id, wallet, token, block_number);

-- Process block range: compute deltas and update balances for all blocks in range
INSERT INTO erc20_balances (chain_id, wallet, token, block_number, balance)
WITH
-- Compute all deltas for all blocks in the range
deltas_all_blocks AS (
    -- Incoming transfers (to address)
    SELECT 
        block_number,
        substring(topic2, 13, 20) as wallet,
        address as token,
        CAST(reinterpretAsUInt256(reverse(data)) AS Int256) as delta
    FROM raw_logs
    WHERE chain_id = @chain_id
      AND block_number >= @from_block
      AND block_number <= @to_block
      AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
      AND length(data) = 32
      AND topic2 IS NOT NULL
      AND substring(topic2, 13, 20) != unhex('0000000000000000000000000000000000000000')
      
    UNION ALL
    
    -- Outgoing transfers (from address)
    SELECT 
        block_number,
        substring(topic1, 13, 20) as wallet,
        address as token,
        -CAST(reinterpretAsUInt256(reverse(data)) AS Int256) as delta
    FROM raw_logs
    WHERE chain_id = @chain_id
      AND block_number >= @from_block
      AND block_number <= @to_block
      AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
      AND length(data) = 32
      AND topic1 IS NOT NULL
      AND substring(topic1, 13, 20) != unhex('0000000000000000000000000000000000000000')
),
-- Sum deltas by wallet/token/block
block_deltas AS (
    SELECT 
        block_number,
        wallet,
        token,
        sum(delta) as net_delta
    FROM deltas_all_blocks
    GROUP BY block_number, wallet, token
),
-- Get starting balance for each affected wallet/token (balance at from_block - 1)
starting_balances AS (
    SELECT 
        wallet,
        token,
        argMax(balance, block_number) as starting_balance
    FROM erc20_balances FINAL
    WHERE chain_id = @chain_id
      AND block_number < @from_block
      AND (wallet, token) IN (SELECT DISTINCT wallet, token FROM block_deltas)
    GROUP BY wallet, token
),
-- Compute cumulative balances using window functions
cumulative_balances AS (
    SELECT 
        bd.block_number,
        bd.wallet,
        bd.token,
        coalesce(sb.starting_balance, toUInt256(0)) as starting_balance,
        sum(bd.net_delta) OVER (
            PARTITION BY bd.wallet, bd.token 
            ORDER BY bd.block_number
        ) as cumulative_delta
    FROM block_deltas bd
    LEFT JOIN starting_balances sb ON sb.wallet = bd.wallet AND sb.token = bd.token
)
-- Calculate final balances
SELECT 
    {chain_id} as chain_id,
    wallet,
    token,
    block_number,
    CAST(starting_balance + cumulative_delta AS UInt256) as balance
FROM cumulative_balances;

-- ===========================================
-- QUERY EXAMPLES
-- ===========================================
-- Get balance at specific block:
-- SELECT balance FROM erc20_balances FINAL
-- WHERE chain_id = ? AND wallet = ? AND token = ? AND block_number <= ?
-- ORDER BY block_number DESC LIMIT 1

-- Get all token balances for a wallet:
-- SELECT token, argMax(balance, block_number) as balance
-- FROM erc20_balances FINAL  
-- WHERE chain_id = ? AND wallet = ? AND block_number <= ?
-- GROUP BY token
-- HAVING balance > 0
