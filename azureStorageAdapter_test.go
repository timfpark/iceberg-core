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

func TestAzureStorageAdapterQuery(t *testing.T) {
	log.Println("Starting TestAzureStorageAdapterQuery")

	fixtureMap := GetFixtureMap()
	beforeTimestamp := fixtureMap["timestamp"].(int64) - 50
	afterTimestamp := fixtureMap["timestamp"].(int64) + 50

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

	err := azureStorageAdapter.Start()
	if err != nil {
		t.Errorf("AzureStorageAdapter failed to start: %s", err)
	}

	results, err := azureStorageAdapter.Query(fixtureMap["user_id"].(string), beforeTimestamp, afterTimestamp)

	if err != nil {
		t.Errorf("AzureStorageAdapter query failed with error: %s", err)
	}

	if len(results) != 1 {
		t.Errorf("AzureStorageAdapter query results list wrong length %d vs. 1", len(results))
	}

	log.Println("Finishing TestAzureStorageAdapterQuery")
}
