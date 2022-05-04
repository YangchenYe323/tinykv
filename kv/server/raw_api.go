package server

import (
	"context"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (response *kvrpcpb.RawGetResponse, err error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)

	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}

	err = nil

	response = &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: len(val) == 0,
	}
	reader.Close()
	return
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (response *kvrpcpb.RawPutResponse, err error) {
	modify := storage.Modify{
		Data: storage.Put{Key: req.Key, Cf: req.Cf, Value: req.Value},
	}
	modifications := []storage.Modify{modify}
	err = server.storage.Write(req.Context, modifications)
	response = &kvrpcpb.RawPutResponse{}
	return
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (response *kvrpcpb.RawDeleteResponse, err error) {
	modify := storage.Modify{
		Data: storage.Delete{Key: req.Key, Cf: req.Cf},
	}
	modifications := []storage.Modify{modify}
	err = server.storage.Write(req.Context, modifications)
	response = &kvrpcpb.RawDeleteResponse{}
	return
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (response *kvrpcpb.RawScanResponse, err error) {
	reader, err := server.storage.Reader(req.Context)

	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)

	pairs := []*kvrpcpb.KvPair{}

	count := 0
	for iter.Valid() {
		if count == int(req.Limit) {
			break
		}

		key := iter.Item().Key()
		val, _ := iter.Item().Value()
		pair := kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		pairs = append(pairs, &pair)
		iter.Next()
		count += 1
	}

	response = &kvrpcpb.RawScanResponse{
		Kvs: pairs,
	}

	return

}
