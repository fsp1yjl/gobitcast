package gobitcast

type Bitcast interface {
	Get(string) (error, string)
	Put(string, string) error
	Delete(string) error
	Merge()
	Close()
}
