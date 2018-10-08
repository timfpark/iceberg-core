package core

type StorageAdapter interface {
	Query(partition interface{}, startKey interface{}, endKey interface{}) []interface{}

	Start() (err error)
	Stop() (err error)
}
