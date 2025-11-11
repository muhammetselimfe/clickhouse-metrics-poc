package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/platformvm/block"
	"github.com/ava-labs/avalanchego/vms/platformvm/txs"
)

func main() {
	client := platformvm.NewClient("http://167.235.8.126:9650")

	latestHeight, err := client.GetHeight(context.Background())
	if err != nil {
		log.Fatalf("Failed to get height: %v", err)
	}
	fmt.Printf("Latest height: %d\n\n", latestHeight)

	// Loop from latest height down to 1
	for height := latestHeight; height >= 1; height-- {
		blockBytes, err := client.GetBlockByHeight(context.Background(), height)
		if err != nil {
			log.Printf("Failed to get block at height %d: %v", height, err)
			continue
		}

		// Parse the block from bytes
		blk, err := block.Parse(block.Codec, blockBytes)
		if err != nil {
			log.Printf("Failed to parse block at height %d: %v", height, err)
			continue
		}

		fmt.Printf("Block ID: %s\n", blk.ID())
		fmt.Printf("Block Height: %d\n", blk.Height())
		fmt.Printf("Parent ID: %s\n", blk.Parent())
		fmt.Printf("Number of transactions: %d\n", len(blk.Txs()))

		// Iterate over transactions
		for _, tx := range blk.Txs() {

			// Access the unsigned transaction
			unsignedTx := tx.Unsigned

			// Check the type of the unsigned transaction

			// Type switch to handle specific transaction types
			switch utx := unsignedTx.(type) {
			case *txs.ConvertSubnetToL1Tx:
				fmt.Printf("ConvertSubnetToL1Tx:\n")
				fmt.Printf("  Subnet ID: %s\n", utx.Subnet)
				fmt.Printf("  Chain ID: %s\n", utx.ChainID)
				fmt.Printf("  Address: %x\n", utx.Address)
				fmt.Printf("  Validators: %d\n", len(utx.Validators))
			case *txs.AddValidatorTx:
				fmt.Printf("AddValidatorTx:\n")
				fmt.Printf("  NodeID: %s\n", utx.Validator.NodeID)
				fmt.Printf("  Start: %d\n", utx.Validator.Start)
				fmt.Printf("  End: %d\n", utx.Validator.End)
				fmt.Printf("  Weight: %d\n", utx.Validator.Wght)
			case *txs.AddDelegatorTx:
				fmt.Printf("AddDelegatorTx:\n")
				fmt.Printf("  NodeID: %s\n", utx.Validator.NodeID)
				fmt.Printf("  Start: %d\n", utx.Validator.Start)
				fmt.Printf("  End: %d\n", utx.Validator.End)
				fmt.Printf("  Weight: %d\n", utx.Validator.Wght)
			case *txs.BaseTx:
				fmt.Printf("BaseTx\n")
			case *txs.CreateSubnetTx:
				fmt.Printf("CreateSubnetTx:\n")
				fmt.Printf("  Owner: %s\n", utx.Owner)
			case *txs.CreateChainTx:
				fmt.Printf("CreateChainTx:\n")
				fmt.Printf("  SubnetID: %s\n", utx.SubnetID)
				fmt.Printf("  ChainName: %s\n", utx.ChainName)
			case *txs.ImportTx:
				fmt.Printf("ImportTx:\n")
				fmt.Printf("  SourceChain: %s\n", utx.SourceChain)
			case *txs.ExportTx:
				fmt.Printf("ExportTx:\n")
				fmt.Printf("  DestinationChain: %s\n", utx.DestinationChain)
			case *txs.AddSubnetValidatorTx:
				fmt.Printf("AddSubnetValidatorTx:\n")
				fmt.Printf("  Subnet: %s\n", utx.SubnetValidator.Subnet)
				fmt.Printf("  NodeID: %s\n", utx.SubnetValidator.NodeID)
			case *txs.RemoveSubnetValidatorTx:
				fmt.Printf("RemoveSubnetValidatorTx:\n")
				fmt.Printf("  Subnet: %s\n", utx.Subnet)
				fmt.Printf("  NodeID: %s\n", utx.NodeID)
			case *txs.TransformSubnetTx:
				fmt.Printf("TransformSubnetTx:\n")
				fmt.Printf("  Subnet: %s\n", utx.Subnet)
			case *txs.TransferSubnetOwnershipTx:
				fmt.Printf("TransferSubnetOwnershipTx:\n")
				fmt.Printf("  Subnet: %s\n", utx.Subnet)
			case *txs.AddPermissionlessValidatorTx:
				fmt.Printf("AddPermissionlessValidatorTx:\n")
				fmt.Printf("  Subnet: %s\n", utx.Subnet)
				fmt.Printf("  NodeID: %s\n", utx.Validator.NodeID)
			case *txs.AddPermissionlessDelegatorTx:
				fmt.Printf("AddPermissionlessDelegatorTx:\n")
				fmt.Printf("  Subnet: %s\n", utx.Subnet)
				fmt.Printf("  NodeID: %s\n", utx.Validator.NodeID)
			case *txs.RewardValidatorTx:
				fmt.Printf("RewardValidatorTx:\n")
				fmt.Printf("  TxID: %s\n", utx.TxID)
			default:
				panic(fmt.Sprintf("Unknown transaction type: %T", utx))
			}
			fmt.Printf("\n")
		}
	}
}
