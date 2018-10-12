package core

import (
	"log"
	"os"
	"testing"
)

func TestAzureStorageAdapterWrite(t *testing.T) {
	log.Println("Starting TestAzureStorageAdapterWrite")

	fixtureMap := GetFixtureMap()

	input := make(chan *Block)

	azureStorageAdapter := &AzureStorageAdapter{
		StorageAccount: os.Getenv("ICEBERG_STORAGE_ACCOUNT"),
		AccessKey:      os.Getenv("ICEBERG_STORAGE_KEY"),
		Container:      "test",

		PartitionColumn: "user_id",
		KeyColumn:       "timestamp",
		CompressionName: "snappy",
		Codec:           GetCodecFixture(),

		Input: input,
	}

	if err := azureStorageAdapter.Start(); err != nil {
		t.Errorf("AzureStorageAdapter failed to start: %s", err)
	}

	block := NewBlock(fixtureMap["user_id"].(string), azureStorageAdapter.KeyColumn, azureStorageAdapter.Codec)
	native := GetNativeFixture()

	block.Write(native)

	input <- block
	close(input)

	if err := azureStorageAdapter.Stop(); err != nil {
		t.Errorf("AzureStorageAdapter failed to stop: %s", err)
	}

	log.Println("Finishing TestAzureStorageAdapterWrite")
}
