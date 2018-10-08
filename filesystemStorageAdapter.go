package core

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	goavro "gopkg.in/linkedin/goavro.v2"
)

type FilesystemStorageAdapter struct {
	BasePath        string
	Codec           *goavro.Codec
	PartitionColumn string
	KeyColumn       string
	CompressionName string
	Input           chan *Block
	running         bool
}

func (fsa *FilesystemStorageAdapter) getPartitionKeyPath(partitionKey string, keyColumn string) string {
	return fmt.Sprintf("%s/%s/%s", fsa.BasePath, partitionKey, keyColumn)
}

func (fsa *FilesystemStorageAdapter) processBlocks() {
	go func() {
		for {
			block, more := <-fsa.Input

			if !more {
				fsa.running = false
				break
			}

			log.Printf("Writing block PartitionKey: %s StartingKey: %d EndingKey: %d with %d rows\n", block.PartitionKey, block.StartingKey, block.EndingKey, len(block.Rows))

			partitionPath := fsa.getPartitionKeyPath(block.PartitionKey, block.KeyColumn)
			blockFilename := block.GetFilename()
			blockFilePath := fmt.Sprintf("%s/%s", partitionPath, blockFilename)

			os.MkdirAll(partitionPath, os.ModePerm)

			toFile, err := os.Create(blockFilePath)
			if err != nil {
				fmt.Printf("creating file failed with %s", err)
				fsa.Input <- block
				return
			}

			defer func(ioc io.Closer) {
				if err := ioc.Close(); err != nil {
					fmt.Printf("closing file failed with %s", err)
				}
			}(toFile)

			ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
				W:               toFile,
				CompressionName: fsa.CompressionName,
				Schema:          fsa.Codec.Schema(),
			})

			err = ocfWriter.Append(block.Rows)
		}
	}()
}

func convertBlockKeyToType(key interface{}, blockKeyString string) (convertedKey interface{}, err error) {
	switch t := key.(type) {
	case int64:
		convertedKey, err = strconv.ParseInt(blockKeyString, 10, 64)
	case string:
		convertedKey = blockKeyString
	default:
		fmt.Printf("unsupported type: %T", t)
	}

	return
}

func (fsa *FilesystemStorageAdapter) Load(partitionKey string, blockFilename string, blocks chan *Block, errors chan error) {
	partitionPath := fsa.getPartitionKeyPath(partitionKey, fsa.KeyColumn)
	blockFilePath := fmt.Sprintf("%s/%s", partitionPath, blockFilename)

	file, err := os.Open(blockFilePath)
	if err != nil {
		errors <- err
		return
	}

	block := &Block{
		Codec:        fsa.Codec,
		Rows:         []interface{}{},
		PartitionKey: partitionKey,
		KeyColumn:    fsa.KeyColumn,
	}

	rows := make(chan interface{})
	ReadOCFIntoChannel(file, rows, errors)

	for {
		row, more := <-rows
		if !more {
			break
		}
		block.Write(row)
	}

	blocks <- block

}

func (fsa *FilesystemStorageAdapter) Query(partitionKey string, startKey interface{}, endKey interface{}) (results []interface{}, err error) {
	partitionPath := fsa.getPartitionKeyPath(partitionKey, fsa.KeyColumn)

	partitionFileInfos, err := ioutil.ReadDir(partitionPath)
	if err != nil {
		errorText := fmt.Sprintf("Partition path %s not found", partitionPath)
		err = errors.New(errorText)
		return
	}

	blockFilenames := make([]string, 0)
	for _, blockFileInfo := range partitionFileInfos {
		blockFilenames = append(blockFilenames, blockFileInfo.Name())
	}

	intersectingBlockFilenames := IntersectingBlockFilenames(blockFilenames, startKey, endKey)

	blocks := make(chan *Block)
	errors := make(chan error)

	results = make([]interface{}, 0)
	for _, intersectingBlockFilename := range intersectingBlockFilenames {
		go fsa.Load(partitionKey, intersectingBlockFilename, blocks, errors)
	}

	for i := 0; i < len(intersectingBlockFilenames); i++ {
		select {
		case block := <-blocks:
			filteredRows := block.RowsForKeyRange(startKey, endKey)
			results = append(results, filteredRows)
		case err = <-errors:
		}
	}

	return
}

func (fsa *FilesystemStorageAdapter) Start() (err error) {
	fsa.running = true
	fsa.processBlocks()

	return nil
}

func (fsa *FilesystemStorageAdapter) Stop() (err error) {
	log.Println("FilesystemStorageAdapter stopping")
	close(fsa.Input)

	// wait for completion
	ttl := 100
	for fsa.running && ttl > 0 {
		ttl--
		log.Printf("FilesystemStorageAdapter waiting for finish: ttl: %d\n", ttl)
		time.Sleep(200 * time.Millisecond)
	}

	if len(fsa.Input) > 0 {
		errorText := fmt.Sprintf("FilesystemStorageAdapter: input did not finish, still has %d blocks remaining", len(fsa.Input))
		return errors.New(errorText)
	}

	return

}
