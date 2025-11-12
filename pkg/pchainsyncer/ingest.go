package pchainsyncer

import (
	"clickhouse-metrics-poc/pkg/pchainrpc"
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ava-labs/avalanchego/ids"
)

// idToBytes converts an ids.ID to a byte slice
func idToBytes(id ids.ID) []byte {
	return id[:]
}

// InsertPChainTxs inserts P-chain transaction data into the p_chain_txs table
func InsertPChainTxs(ctx context.Context, conn clickhouse.Conn, pchainID uint32, blocks []*pchainrpc.JSONBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO p_chain_txs (
		tx_id, tx_type, block_number, block_time, p_chain_id, tx_data
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, block := range blocks {
		for _, tx := range block.Transactions {
			txID := idToBytes(tx.TxID)

			err = batch.Append(
				txID,
				tx.TxType,
				tx.BlockHeight,
				tx.BlockTime,
				pchainID,
				string(tx.TxData), // ClickHouse JSON type expects string
			)
			if err != nil {
				return fmt.Errorf("failed to append tx %s: %w", tx.TxID, err)
			}
		}
	}

	return batch.Send()
}
