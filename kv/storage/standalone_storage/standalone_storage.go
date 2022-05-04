package standalone_storage

import (
	"log"

	badger "github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Open the Badger database located in the conf.DBPath directory
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (reader storage.StorageReader, err error) {
	txn := s.db.NewTransaction(false)
	reader = &StandAloneStorageReader{txn: txn}
	return
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) (err error) {
	txn := s.db.NewTransaction(true)
	for _, modification := range batch {
		switch modification.Data.(type) {
		case storage.Put:
			key := modification.Key()
			val := modification.Value()
			cf := modification.Cf()
			err = txn.Set(engine_util.KeyWithCF(cf, key), val)

		case storage.Delete:
			key := modification.Key()
			cf := modification.Cf()
			txn.Delete(engine_util.KeyWithCF(cf, key))
		}
	}
	txn.Commit()
	return
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) (val []byte, err error) {
	item, err := s.txn.Get(engine_util.KeyWithCF(cf, key))
	if item == nil {
		err = nil
		return
	}
	if !item.IsEmpty() {
		val, err = item.ValueCopy(val)
	}
	return
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Commit()
}
