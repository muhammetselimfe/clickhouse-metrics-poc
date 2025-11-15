-- ERC-20 Balance Tracking - Separate Credits/Debits
-- Stores: chain_id, wallet, token, total_in, total_out, last_updated_block
-- Balance = total_in - total_out (computed at query time)
-- Uses ReplacingMergeTree for idempotency

CREATE TABLE IF NOT EXISTS erc20_balances (
    chain_id UInt32,
    wallet FixedString(20),
    token FixedString(20),
    total_in UInt256,
    total_out UInt256,
    last_updated_block UInt32,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, wallet, token);

-- Process block range: accumulate credits and debits
-- Start from max(last_updated_block) to ensure idempotency
INSERT INTO erc20_balances (chain_id, wallet, token, total_in, total_out, last_updated_block)
WITH 
-- Get the max last_updated_block across all wallets/tokens for this chain
max_block AS (
    SELECT coalesce(max(last_updated_block), 0) as start_block
    FROM erc20_balances FINAL
    WHERE chain_id = @chain_id
)
SELECT 
    @chain_id as chain_id,
    agg.wallet,
    agg.token,
    coalesce(old.total_in, toUInt256(0)) + agg.total_in as total_in,
    coalesce(old.total_out, toUInt256(0)) + agg.total_out as total_out,
    @to_block as last_updated_block
FROM (
    SELECT 
        wallet,
        token,
        sum(if(is_incoming, amount, toUInt256(0))) as total_in,
        sum(if(NOT is_incoming, amount, toUInt256(0))) as total_out
    FROM (
        -- ========================================================================
        -- INCOMING BALANCE CHANGES
        -- ========================================================================
        
        -- Standard ERC20 incoming transfers (Transfer event with wallet as recipient)
        SELECT 
            substring(topic2, 13, 20) as wallet,
            address as token,
            reinterpretAsUInt256(reverse(data)) as amount,
            true as is_incoming
        FROM raw_logs
        CROSS JOIN max_block
        WHERE chain_id = @chain_id
          AND block_number > max_block.start_block
          AND block_number <= @to_block
          AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')  -- Transfer(address,address,uint256)
          AND length(data) = 32
          AND topic2 IS NOT NULL
          AND substring(topic2, 13, 20) != unhex('0000000000000000000000000000000000000000')
          
        UNION ALL
        
        -- CRITICAL: Wrapped token deposits (WAVAX)
        -- When users deposit native currency (AVAX, ETH, MATIC) they receive wrapped tokens
        -- These emit Deposit(address indexed dst, uint wad) instead of Transfer from zero
        -- Example: WAVAX has 25+ million Deposit events that MUST be counted as balance increases
        -- If this is removed, wrapped token balances will show massive underflows!
        SELECT 
            substring(topic1, 13, 20) as wallet,  -- topic1 contains the destination address
            address as token,
            reinterpretAsUInt256(reverse(data)) as amount,
            true as is_incoming
        FROM raw_logs
        CROSS JOIN max_block
        WHERE chain_id = @chain_id
          AND block_number > max_block.start_block
          AND block_number <= @to_block
          AND topic0 = unhex('e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c')  -- Deposit(address,uint256)
          AND length(data) = 32
          AND topic1 IS NOT NULL
          AND substring(topic1, 13, 20) != unhex('0000000000000000000000000000000000000000')
          
        UNION ALL
        
        -- ========================================================================
        -- OUTGOING BALANCE CHANGES
        -- ========================================================================
        
        -- Standard ERC20 outgoing transfers (Transfer event with wallet as sender)
        SELECT 
            substring(topic1, 13, 20) as wallet,
            address as token,
            reinterpretAsUInt256(reverse(data)) as amount,
            false as is_incoming
        FROM raw_logs
        CROSS JOIN max_block
        WHERE chain_id = @chain_id
          AND block_number > max_block.start_block
          AND block_number <= @to_block
          AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')  -- Transfer(address,address,uint256)
          AND length(data) = 32
          AND topic1 IS NOT NULL
          AND substring(topic1, 13, 20) != unhex('0000000000000000000000000000000000000000')
          
        UNION ALL
        
        -- CRITICAL: Wrapped token withdrawals (WAVAX)
        -- When users withdraw/unwrap tokens back to native currency, tokens are burned
        -- These emit Withdrawal(address indexed src, uint wad) instead of Transfer to zero
        -- Example: WAVAX has 26+ million Withdrawal events that MUST be counted as balance decreases
        -- If this is removed, wrapped token balances will show incorrect positive balances!
        SELECT 
            substring(topic1, 13, 20) as wallet,  -- topic1 contains the source address
            address as token,
            reinterpretAsUInt256(reverse(data)) as amount,
            false as is_incoming
        FROM raw_logs
        CROSS JOIN max_block
        WHERE chain_id = @chain_id
          AND block_number > max_block.start_block
          AND block_number <= @to_block
          AND topic0 = unhex('7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65')  -- Withdrawal(address,uint256)
          AND length(data) = 32
          AND topic1 IS NOT NULL
          AND substring(topic1, 13, 20) != unhex('0000000000000000000000000000000000000000')
    ) transfers
    GROUP BY wallet, token
) agg
LEFT JOIN (
    SELECT wallet, token, total_in, total_out, last_updated_block
    FROM erc20_balances FINAL
    WHERE chain_id = @chain_id
) old ON agg.wallet = old.wallet AND agg.token = old.token;

-- ===========================================
-- QUERY EXAMPLES
-- ===========================================
-- Get current balance:
-- SELECT total_in - total_out as balance 
-- FROM erc20_balances FINAL
-- WHERE chain_id = ? AND wallet = ? AND token = ?

-- Get all token balances for a wallet:
-- SELECT token, total_in - total_out as balance
-- FROM erc20_balances FINAL  
-- WHERE chain_id = ? AND wallet = ? 
-- HAVING balance > 0

-- Get top holders of a token:
-- SELECT wallet, total_in - total_out as balance
-- FROM erc20_balances FINAL
-- WHERE chain_id = ? AND token = ?
-- ORDER BY balance DESC
-- LIMIT 100
