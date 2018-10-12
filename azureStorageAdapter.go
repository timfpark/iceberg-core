package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	goavro "gopkg.in/linkedin/goavro.v2"
)

type AzureStorageAdapter struct {
	StorageAccount string
	AccessKey      string
	Container      string

	Codec           *goavro.Codec
	PartitionColumn string
	KeyColumn       string
	CompressionName string

	Input chan *Block

	containerURL azblob.ContainerURL
	context      context.Context
	running      bool
}

func (asa *AzureStorageAdapter) uploadBlock(block *Block) (err error) {
	log.Printf("Uploading block PartitionKey: %s StartingKey: %d EndingKey: %d with %d rows\n", block.PartitionKey, block.StartingKey, block.EndingKey, len(block.Rows))

	avroBuffer := new(bytes.Buffer)

	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               avroBuffer,
		CompressionName: asa.CompressionName,
		Schema:          asa.Codec.Schema(),
	})

	if err != nil {
		return err
	}

	if err = ocfWriter.Append(block.Rows); err != nil {
		return err
	}

	blobPath := asa.buildBlobPath(block.PartitionKey, block.KeyColumn)
	blobFilePath := fmt.Sprintf("%s/%s", blobPath, block.GetFilename())
	blobURL := asa.containerURL.NewBlockBlobURL(blobFilePath)

	avroBytes := avroBuffer.Bytes()

	_, err = azblob.UploadBufferToBlockBlob(asa.context, avroBytes, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})

	log.Printf("Uploading block PartitionKey: %s StartingKey: %d EndingKey: %d finished\n", block.PartitionKey, block.StartingKey, block.EndingKey)
	return err
}

func (asa *AzureStorageAdapter) buildBlobPath(partitionKey string, keyColumn string) string {
	return fmt.Sprintf("%s/%s", partitionKey, keyColumn)
}

func (asa *AzureStorageAdapter) processBlocks() {
	go func() {
		for {
			block, more := <-asa.Input

			if !more {
				log.Printf("Input has closed, emitting adapter finished\n")
				asa.running = false
				break
			}

			if err := asa.uploadBlock(block); err != nil {
				log.Printf("Uploading block failed PartitionKey: %s StartingKey: %d EndingKey: %d with %s. Retrying.\n", block.PartitionKey, block.StartingKey, block.EndingKey, err)

				asa.Input <- block
			}
		}
	}()
}

func (asa *AzureStorageAdapter) Load(partitionKey string, blockFilename string, blocks chan *Block, errors chan error) {
	blobPath := asa.buildBlobPath(partitionKey, asa.KeyColumn)
	blobFilePath := fmt.Sprintf("%s/%s", blobPath, blockFilename)
	blobURL := asa.containerURL.NewBlockBlobURL(blobFilePath)

	stream := azblob.NewDownloadStream(asa.context, blobURL.GetBlob, azblob.DownloadStreamOptions{})

	block := &Block{
		Codec:        asa.Codec,
		Rows:         []interface{}{},
		PartitionKey: partitionKey,
		KeyColumn:    asa.KeyColumn,
	}

	rows := make(chan interface{})
	ReadOCFIntoChannel(stream, rows, errors)

	for {
		row, more := <-rows
		if !more {
			break
		}
		block.Write(row)
	}

	blocks <- block
}

func (asa *AzureStorageAdapter) GetPartitionFileNames(partitionKey string) (partitionFileNames []string, err error) {
	partitionFileNames = []string{}
	partitionPath := asa.buildBlobPath(partitionKey, asa.KeyColumn)

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := asa.containerURL.ListBlobs(asa.context, marker, azblob.ListBlobsOptions{
			Prefix: partitionPath,
		})

		if err != nil {
			return nil, err
		}

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Blobs.Blob {
			nameParts := strings.Split(blobInfo.Name, "/")
			if len(nameParts) == 3 {
				partitionFileNames = append(partitionFileNames, nameParts[2])
			}
		}
	}

	return partitionFileNames, nil
}

func (asa *AzureStorageAdapter) Query(partitionKey string, startKey interface{}, endKey interface{}) (results []interface{}, err error) {
	partitionFileNames, err := asa.GetPartitionFileNames(partitionKey)
	intersectingPartitionFilenames := IntersectingBlockFilenames(partitionFileNames, startKey, endKey)

	blocks := make(chan *Block)
	errors := make(chan error)

	results = make([]interface{}, 0)
	for _, intersectingPartitionFilename := range intersectingPartitionFilenames {
		go asa.Load(partitionKey, intersectingPartitionFilename, blocks, errors)
	}

	for i := 0; i < len(intersectingPartitionFilenames); i++ {
		select {
		case block := <-blocks:
			filteredRows := block.RowsForKeyRange(startKey, endKey)
			results = append(results, filteredRows)
		case err = <-errors:
			return results, err
		}
	}

	return
}

func (asa *AzureStorageAdapter) Start() (err error) {
	if len(asa.StorageAccount) == 0 {
		return errors.New("AzureStorageAdapter not correctly configured with StorageAccount")
	}

	if len(asa.AccessKey) == 0 {
		return errors.New("AzureStorageAdapter not correctly configured with AccessKey")
	}

	if len(asa.Container) == 0 {
		return errors.New("AzureStorageAdapter not correctly configured with Container")
	}

	credential := azblob.NewSharedKeyCredential(asa.StorageAccount, asa.AccessKey)
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	URL, err := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", asa.StorageAccount, asa.Container))

	if err != nil {
		return err
	}

	asa.containerURL = azblob.NewContainerURL(*URL, pipeline)
	asa.context = context.Background()

	asa.containerURL.Create(asa.context, azblob.Metadata{}, azblob.PublicAccessNone)

	asa.processBlocks()
	asa.running = true

	return nil
}

func (asa *AzureStorageAdapter) Stop() (err error) {
	log.Println("AzureStorageAdapter stopping")

	// wait for completion
	ttl := 100
	for asa.running && ttl > 0 {
		ttl--
		log.Printf("AzureStorageAdapter waiting for finish: ttl: %d\n", ttl)
		time.Sleep(200 * time.Millisecond)
	}

	if len(asa.Input) > 0 {
		errorText := fmt.Sprintf("AzureStorageAdapter: input did not finish, still has %d blocks remaining", len(asa.Input))
		return errors.New(errorText)
	}

	log.Printf("AzureStorageAdapter stopped\n")

	return nil
}
