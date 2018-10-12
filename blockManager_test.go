package core

import (
	"log"
	"testing"
	"time"
)

func TestBlockManager(t *testing.T) {
	log.Println("Starting TestBlockManager")

	input := make(chan interface{})
	output := make(chan *Block)

	native := GetNativeFixture()

	blockManager := &BlockManager{
		MaxAge:          1000, // milliseconds
		MaxSize:         8192, // rows
		PartitionColumn: "user_id",
		KeyColumn:       "timestamp",
		Input:           input,
		Output:          output,
		Codec:           GetCodecFixture(),
	}

	err := blockManager.Start()
	if err != nil {
		t.Errorf("Block Manager start failed with error: %s", err)
	}

	start := time.Now()

	input <- native
	input <- native
	block := <-output

	ageMillis := uint32(time.Now().Sub(start).Seconds() * 1000.0)

	if ageMillis < blockManager.MaxAge {
		t.Errorf("Block was committed too early: %d vs. %d", ageMillis, blockManager.MaxAge)
	}

	if block.Length() != 2 {
		t.Errorf("Block is the wrong size: %d vs. 2", block.Length())
	}

	log.Println("Finished TestBlockManager")
}
