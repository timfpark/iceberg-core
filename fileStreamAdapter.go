package core

import (
	"fmt"
	"os"

	goavro "gopkg.in/linkedin/goavro.v2"
)

type FileStreamAdapter struct {
	FilePath string

	Codec *goavro.Codec

	Output chan interface{}
	Errors chan error
}

func (fsa *FileStreamAdapter) removeTypeMaps(native interface{}) (flattened map[string]interface{}) {
	flattened = make(map[string]interface{})

	nativeMap := native.(map[string]interface{})
	for key, value := range nativeMap {
		switch t := value.(type) {
		case map[string]interface{}:
			valueMap := value.(map[string]interface{})
			for _, value2 := range valueMap {
				flattened[key] = value2
			}
		case nil:
			flattened[key] = nil
		default:
			fmt.Printf("unsupported type: %T\n", t)
		}
	}

	return flattened
}

func (fsa *FileStreamAdapter) Start() (err error) {
	file, err := os.Open(fsa.FilePath)
	if err != nil {
		return
	}

	ReadOCFIntoChannel(file, fsa.Output, fsa.Errors)

	return nil
}

func (fsa *FileStreamAdapter) Stop() (err error) {
	return nil
}
