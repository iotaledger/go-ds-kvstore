package kvstoreds

import (
	"testing"

	dstest "github.com/ipfs/go-datastore/test"

	"github.com/iotaledger/hive.go/kvstore/mapdb"
)

func TestKVStoreDatastore(t *testing.T) {
	ds, cleanup := newDatastore(t)
	defer cleanup()

	dstest.SubtestAll(t, ds)
}

func newDatastore(t *testing.T) (*Datastore, func()) {
	t.Helper()

	d := NewDatastore(mapdb.NewMapDB())

	return d, func() {
		_ = d.Close()
	}
}
