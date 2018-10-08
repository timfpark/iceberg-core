package core

import (
	"log"
	"testing"
)

func TestBlockWrite(t *testing.T) {
	log.Println("Starting TestBlockWrite")

	block := NewBlock("userid1", "timestamp", GetCodecFixture())
	native := GetNativeFixture()

	block.Write(native)

	if block.Length() != 1 {
		t.Errorf("Row length after write incorrect: %d", block.Length())
	}

	log.Println("Finished TestBlockWrite")
}
