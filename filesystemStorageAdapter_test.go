package core

import (
	"log"
	"testing"
)

func TestFilesystemStorageAdapterWrite(t *testing.T) {
	log.Println("Starting TestFilesystemStorageAdapterWrite")

	fixtureMap := GetFixtureMap()

	input := make(chan *Block)

	filesystemStorageAdapter := &FilesystemStorageAdapter{
		BasePath:        "./test/data",
		Codec:           GetCodecFixture(),
		PartitionColumn: "user_id",
		KeyColumn:       "timestamp",
		CompressionName: "snappy",
		Input:           input,
	}

	err := filesystemStorageAdapter.Start()
	if err != nil {
		t.Errorf("filesystemStorageAdapter failed to start: %s", err)
	}

	block := NewBlock(fixtureMap["user_id"].(string), filesystemStorageAdapter.KeyColumn, filesystemStorageAdapter.Codec)
	native := GetNativeFixture()

	block.Write(native)

	input <- block
	close(input)

	filesystemStorageAdapter.Stop()

	log.Println("Finishing TestFilesystemStorageAdapterWrite")
}

func TestFilesystemStorageAdapterQuery(t *testing.T) {
	log.Println("Starting TestFilesystemStorageAdapterQuery")

	fixtureMap := GetFixtureMap()
	beforeTimestamp := fixtureMap["timestamp"].(int64) - 50
	afterTimestamp := fixtureMap["timestamp"].(int64) + 50

	input := make(chan *Block)
	filesystemStorageAdapter := &FilesystemStorageAdapter{
		BasePath:        "./test/data",
		Codec:           GetCodecFixture(),
		PartitionColumn: "user_id",
		KeyColumn:       "timestamp",
		CompressionName: "snappy",
		Input:           input,
	}

	err := filesystemStorageAdapter.Start()
	if err != nil {
		t.Errorf("filesystemStorageAdapter failed to start: %s", err)
	}

	results, err := filesystemStorageAdapter.Query(fixtureMap["user_id"].(string), beforeTimestamp, afterTimestamp)

	if err != nil {
		t.Errorf("filesystemStorageAdapter query failed with error: %s", err)
	}

	if len(results) != 1 {
		t.Errorf("filesystemStorageAdapter query results list wrong length %d vs. 1", len(results))
	}

	log.Println("Finishing TestFilesystemStorageAdapterQuery")
}
