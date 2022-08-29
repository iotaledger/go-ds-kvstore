package kvstoreds_test

import (
	"testing"

	dstest "github.com/ipfs/go-datastore/test"

	kvstoreds "github.com/iotaledger/go-ds-kvstore"
	"github.com/iotaledger/hive.go/core/kvstore/mapdb"
)

func TestKVStoreDatastore(t *testing.T) {
	ds, cleanup := newDatastore(t)
	defer cleanup()

	dstest.SubtestAll(t, ds)
}

func newDatastore(t *testing.T) (*kvstoreds.Datastore, func()) {
	t.Helper()

	d := kvstoreds.NewDatastore(mapdb.NewMapDB())

	return d, func() {
		_ = d.Close()
	}
}
