package main

// NOTE:
// Go maps do not have a size cap. Use LRU-caches instead for anything serious.

import (
	"sync"
	"time"

	"github.com/faroedev/faroe"
)

type storageStruct struct {
	records map[string]storageRecordStruct
	m       *sync.Mutex
}

func newStorage() *storageStruct {
	storage := &storageStruct{
		records: map[string]storageRecordStruct{},
		m:       &sync.Mutex{},
	}
	return storage
}

func (storage *storageStruct) Get(key string) ([]byte, int32, error) {
	storage.m.Lock()
	defer storage.m.Unlock()

	record, ok := storage.records[key]
	if !ok {
		return nil, 0, faroe.ErrStorageEntryNotFound
	}

	return record.value, record.counter, nil
}

func (storage *storageStruct) Add(key string, value []byte, _ time.Time) error {
	storage.m.Lock()
	defer storage.m.Unlock()

	_, ok := storage.records[key]
	if ok {
		return faroe.ErrStorageEntryAlreadyExists
	}

	storage.records[key] = storageRecordStruct{
		key:     key,
		value:   value,
		counter: 0,
	}

	return nil
}

func (storage *storageStruct) Update(key string, value []byte, _ time.Time, counter int32) error {
	storage.m.Lock()
	defer storage.m.Unlock()

	record, ok := storage.records[key]
	if !ok {
		return faroe.ErrStorageEntryNotFound
	}
	if record.counter != counter {
		return faroe.ErrStorageEntryNotFound
	}
	record.value = value
	record.counter++
	storage.records[key] = record

	return nil
}

func (storage *storageStruct) Delete(key string) error {
	storage.m.Lock()
	defer storage.m.Unlock()

	_, ok := storage.records[key]
	if !ok {
		return faroe.ErrStorageEntryNotFound
	}
	delete(storage.records, key)

	return nil
}

type storageRecordStruct struct {
	key     string
	value   []byte
	counter int32
}
