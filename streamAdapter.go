package core

import (
	"io"
	"log"

	goavro "gopkg.in/linkedin/goavro.v2"
)

type StreamAdapter interface {
	Start() (err error)
	Stop() (err error)
}

func ReadOCFIntoChannel(reader io.Reader, output chan interface{}, errors chan error) {
	ocf, err := goavro.NewOCFReader(reader)
	if err != nil {
		log.Printf("NewOCFReader error: %s\n", err)
		errors <- err
		close(output)
		return
	}

	go func(ocf *goavro.OCFReader) {
		for ocf.Scan() {
			native, err := ocf.Read()
			if err != nil {
				log.Printf("Read error: %s\n", err)
				errors <- err
				break
			}

			output <- native
		}

		log.Printf("Input finished\n")

		close(output)
	}(ocf)

	return
}
