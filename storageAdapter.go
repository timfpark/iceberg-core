package core

type StorageAdapter interface {
	Query(partitionKey string, startKey interface{}, endKey interface{}) (results []interface{}, err error)

	Start() (err error)
	Stop() (err error)
}
