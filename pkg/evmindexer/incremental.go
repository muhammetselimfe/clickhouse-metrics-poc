package evmindexer

import (
	"fmt"
	"time"
)

const (
	incrementalMinInterval = 900 * time.Millisecond
)

// processIncrementals checks and runs all incremental indexers
func (r *IndexRunner) processIncrementals() {
	for _, indexerFile := range r.incrementalIndexers {
		indexerName := fmt.Sprintf("incremental/%s", indexerFile)

		// Check throttle
		if !r.shouldRun(indexerName, incrementalMinInterval) {
			continue
		}

		watermark := r.getWatermark(indexerName)

		// Initialize to block 1 if never run
		if watermark.LastBlockNum == 0 {
			watermark.LastBlockNum = 1
		}

		// Check if we have new blocks
		if r.latestBlockNum <= watermark.LastBlockNum {
			continue
		}

		// Limit to 20k blocks at a time
		endBlock := r.latestBlockNum
		const maxBlocks = 20000
		if endBlock-watermark.LastBlockNum > maxBlocks {
			endBlock = watermark.LastBlockNum + maxBlocks
		}

		// Run indexer
		fmt.Printf("[Chain %d] Running %s - blocks %d to %d\n",
			r.chainId, indexerName, watermark.LastBlockNum+1, endBlock)

		start := time.Now()
		if err := r.runIncrementalIndexer(indexerFile, watermark.LastBlockNum+1, endBlock); err != nil {
			fmt.Printf("[Chain %d] FATAL: Failed to run %s: %v\n", r.chainId, indexerName, err)
			panic(err)
		}
		elapsed := time.Since(start)
		fmt.Printf("[Chain %d] %s - blocks %d to %d - time taken: %s\n",
			r.chainId, indexerName, watermark.LastBlockNum+1, endBlock, elapsed)

		// Update watermark and last run time
		watermark.LastBlockNum = endBlock
		if err := r.saveWatermark(indexerName, watermark); err != nil {
			fmt.Printf("[Chain %d] FATAL: Failed to save watermark for %s: %v\n", r.chainId, indexerName, err)
			panic(err)
		}
		r.lastRunTime[indexerName] = time.Now()
	}
}

// runIncrementalIndexer executes an incremental indexer for given block range
func (r *IndexRunner) runIncrementalIndexer(indexerFile string, firstBlock, lastBlock uint64) error {
	// Template parameters (string replacement for SELECT clauses)
	templateParams := []struct{ key, value string }{
		{"{chain_id}", fmt.Sprintf("%d", r.chainId)},
	}

	// Bind parameters (native ClickHouse parameter binding for WHERE clauses)
	bindParams := map[string]interface{}{
		"chain_id":    r.chainId,
		"first_block": firstBlock,
		"last_block":  lastBlock,
	}

	filename := fmt.Sprintf("evm_incremental/%s.sql", indexerFile)
	return executeSQLFile(r.conn, r.sqlDir, filename, templateParams, bindParams)
}

// shouldRun checks if enough time has passed since last run
func (r *IndexRunner) shouldRun(indexerName string, minInterval time.Duration) bool {
	lastRun, exists := r.lastRunTime[indexerName]
	if !exists {
		return true
	}
	return time.Since(lastRun) >= minInterval
}
