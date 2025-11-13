package evmindexer

import (
	"fmt"
	"time"
)

// processIncrementalBatch processes all pending blocks for all incremental indexers
// Processes ALL blocks in one shot: from watermark+1 to latestBlockNum
// Returns true if any work was done
func (r *IndexRunner) processIncrementalBatch() bool {
	hasWork := false

	// Process each indexer independently
	for _, indexerFile := range r.incrementalIndexers {
		indexerName := fmt.Sprintf("incremental/%s", indexerFile)
		watermark := r.getWatermark(indexerName)

		// Initialize watermark to startBlock-1 if never run (so first processed block is startBlock)
		if watermark.LastBlockNum == 0 {
			watermark.LastBlockNum = r.startBlock - 1
		}

		// Check if there are blocks to process
		if watermark.LastBlockNum < r.latestBlockNum {
			fromBlock := watermark.LastBlockNum + 1
			toBlock := r.latestBlockNum

			// Run indexer for the entire block range
			start := time.Now()
			if err := r.runIncrementalIndexer(indexerFile, fromBlock, toBlock); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to run %s: %v\n", r.chainId, indexerName, err)
				panic(err)
			}
			elapsed := time.Since(start)

			// Update watermark to the latest block
			watermark.LastBlockNum = toBlock

			// Save watermark to DB
			if err := r.saveWatermark(indexerName, watermark); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to save watermark for %s: %v\n", r.chainId, indexerName, err)
				panic(err)
			}

			// Log the batch processing
			blockCount := toBlock - fromBlock + 1
			fmt.Printf("[Chain %d] %s - processed blocks %d to %d (%d blocks) - %s\n",
				r.chainId, indexerName, fromBlock, toBlock, blockCount, elapsed)

			hasWork = true
		}
	}

	return hasWork
}

// runIncrementalIndexer executes an incremental indexer for a block range
func (r *IndexRunner) runIncrementalIndexer(indexerFile string, fromBlock, toBlock uint64) error {
	// Template parameters (string replacement for SELECT clauses)
	templateParams := []struct{ key, value string }{
		{"{chain_id}", fmt.Sprintf("%d", r.chainId)},
	}

	// Bind parameters (native ClickHouse parameter binding for WHERE clauses)
	bindParams := map[string]interface{}{
		"chain_id":   r.chainId,
		"from_block": fromBlock,
		"to_block":   toBlock,
	}

	filename := fmt.Sprintf("evm_incremental/%s.sql", indexerFile)
	return executeSQLFile(r.conn, r.sqlDir, filename, templateParams, bindParams)
}
