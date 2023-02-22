package kvstoreds

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jbenet/goprocess"

	"github.com/iotaledger/hive.go/kvstore"
)

// Datastore is a github.com/iotaledger/hive.go/core/kvstore backed github.com/ipfs/go-datastore.
type Datastore struct {
	store   kvstore.KVStore
	status  int32
	closing chan struct{}
	wg      sync.WaitGroup
}

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.Batching = (*Datastore)(nil)

// NewDatastore creates a kvstore-backed datastore.
func NewDatastore(store kvstore.KVStore) *Datastore {
	return &Datastore{
		store:   store,
		closing: make(chan struct{}),
	}
}

// Get retrieves the object `value` named by `key`.
// Get will return ErrNotFound if the key is not mapped to a value.
func (d *Datastore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	value, err := d.store.Get(key.Bytes())
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return nil, ds.ErrNotFound
		}

		return nil, fmt.Errorf("kvstore error during Get: %w", err)
	}

	return value, nil
}

// Has returns whether the `key` is mapped to a `value`.
// In some contexts, it may be much cheaper only to check for existence of
// a value, rather than retrieving the value itself.
func (d *Datastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	exists, err := d.store.Has(key.Bytes())
	if err != nil {
		return false, fmt.Errorf("kvstore error during Has: %w", err)
	}

	return exists, nil
}

// GetSize returns the size of the `value` named by `key`.
// In some contexts, it may be much cheaper to only get the size of the
// value rather than retrieving the value itself.
func (d *Datastore) GetSize(ctx context.Context, key ds.Key) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	value, err := d.store.Get(key.Bytes())
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return -1, ds.ErrNotFound
		}

		return -1, fmt.Errorf("kvstore error during Get: %w", err)
	}

	return len(value), nil
}

// handleUnknownQueryOrder is used if the kvstore implementation can't handle
// the queried order, so the naive query logic handles it itself.
func (d *Datastore) handleUnknownQueryOrder(ctx context.Context, q query.Query, baseOrder query.Order) (query.Results, error) {

	// since we can't apply limits and offsets without correct order, we need to
	// skip these things in the base query to get all results.
	baseQuery := q
	baseQuery.Limit = 0
	baseQuery.Offset = 0
	baseQuery.Orders = nil
	if baseOrder != nil {
		baseQuery.Orders = []query.Order{baseOrder}
	}

	// perform the base query.
	res, err := d.Query(ctx, baseQuery)
	if err != nil {
		return nil, err
	}

	// fix the query
	res = query.ResultsReplaceQuery(res, q)

	// Remove the parts we've already applied.
	naiveQuery := q
	naiveQuery.Prefix = ""
	naiveQuery.Filters = nil

	// Apply the rest of the query
	return query.NaiveQueryApply(naiveQuery, res), nil
}

// Query searches the datastore and returns a query result. This function
// may return before the query actually runs. To wait for the query:
//
//	result, _ := ds.Query(q)
//
//	// use the channel interface; result may come in at different times
//	for entry := range result.Next() { ... }
//
//	// or wait for the query to be completely done
//	entries, _ := result.Rest()
//	for entry := range entries { ... }
func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var (
		prefix      = ds.NewKey(q.Prefix).String()
		filters     = q.Filters
		orders      = q.Orders
		limit       = q.Limit
		offset      = q.Offset
		keysOnly    = q.KeysOnly
		_           = q.ReturnExpirations // kvstore doesn't support TTL
		returnSizes = q.ReturnsSizes
	)

	if prefix != "/" {
		prefix += "/"
	}

	var iterDirection kvstore.IterDirection
	switch l := len(orders); l {

	case 0:
		iterDirection = kvstore.IterDirectionForward

	case 1:
		switch o := orders[0]; o.(type) {
		case query.OrderByKey, *query.OrderByKey:
			iterDirection = kvstore.IterDirectionForward
		case query.OrderByKeyDescending, *query.OrderByKeyDescending:
			iterDirection = kvstore.IterDirectionBackward
		default:
			return d.handleUnknownQueryOrder(ctx, q, nil)
		}

	default:
		var baseOrder query.Order
		for _, o := range orders {
			if baseOrder != nil {
				return nil, fmt.Errorf("incompatible orders passed: %+v", orders)
			}
			switch o.(type) {
			case query.OrderByKey, query.OrderByKeyDescending, *query.OrderByKey, *query.OrderByKeyDescending:
				baseOrder = o
			}
		}

		return d.handleUnknownQueryOrder(ctx, q, baseOrder)
	}

	filterFunc := func(entry query.Entry) bool {
		for _, f := range filters {
			if !f.Filter(entry) {
				return false
			}
		}

		return true
	}

	checkFilters := len(filters) > 0

	d.wg.Add(1)
	results := query.ResultsWithProcess(q, func(proc goprocess.Process, outCh chan<- query.Result) {
		defer d.wg.Done()

		const interrupted = "interrupted"

		defer func() {
			switch r := recover(); r {
			case nil, interrupted:
				// no panic or interrupted, do nothing
			default:
				// propagate the panic
				panic(r)
			}
		}()

		sendOrInterrupt := func(r query.Result) {
			select {
			case outCh <- r:
				return
			case <-d.closing:
			case <-proc.Closed():
			case <-proc.Closing(): // client told us to close early
			}

			// we are closing; try to send a closure error to the client.
			// but do not halt because they might have stopped receiving.
			select {
			case outCh <- query.Result{Error: fmt.Errorf("close requested")}:
			default:
			}
			panic(interrupted)
		}

		skipped := 0
		sent := 0

		processEntry := func(entry query.Entry) bool {

			if checkFilters && !filterFunc(entry) {
				// if we have a filter, and this entry doesn't match it,
				// don't count it.
				return true
			}
			skipped++

			// skip over 'offset' entries; if a filter is provided, only entries
			// that match the filter will be counted as a skipped entry.
			if skipped <= offset {
				return true
			}

			// start sending results, capped at limit (if > 0)
			if limit > 0 && sent >= limit {
				return false
			}

			sendOrInterrupt(query.Result{Entry: entry})
			sent++

			return true
		}

		if keysOnly {
			if err := d.store.IterateKeys([]byte(prefix), func(key kvstore.Key) bool {
				return processEntry(query.Entry{Key: string(key)})
			}, iterDirection); err != nil {
				sendOrInterrupt(query.Result{Error: fmt.Errorf("kvstore error during IterateKeys: %w", err)})
			}
		} else {
			if err := d.store.Iterate([]byte(prefix), func(key kvstore.Key, value kvstore.Value) bool {
				entry := query.Entry{Key: string(key), Value: value}
				if returnSizes {
					entry.Size = len(entry.Value)
				}

				return processEntry(entry)
			}, iterDirection); err != nil {
				sendOrInterrupt(query.Result{Error: fmt.Errorf("kvstore error during Iterate: %w", err)})
			}
		}
	})

	return results, nil
}

// Put stores the object `value` named by `key`.
//
// The generalized Datastore interface does not impose a value type,
// allowing various datastore middleware implementations (which do not
// handle the values directly) to be composed together.
//
// Ultimately, the lowest-level datastore will need to do some value checking
// or risk getting incorrect values. It may also be useful to expose a more
// type-safe interface to your application, and do the checking up-front.
func (d *Datastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.store.Set(key.Bytes(), value); err != nil {
		return fmt.Errorf("kvstore error during Set: %w", err)
	}

	return nil
}

// Delete removes the value for given `key`. If the key is not in the
// datastore, this method returns no error.
func (d *Datastore) Delete(ctx context.Context, key ds.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.store.Delete(key.Bytes()); err != nil {
		return fmt.Errorf("kvstore error during Delete: %w", err)
	}

	return nil
}

// Sync guarantees that any Put or Delete calls under prefix that returned
// before Sync(prefix) was called will be observed after Sync(prefix)
// returns, even if the program crashes. If Put/Delete operations already
// satisfy these requirements then Sync may be a no-op.
//
// If the prefix fails to Sync this method returns an error.
func (d *Datastore) Sync(ctx context.Context, _ ds.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.store.Flush(); err != nil {
		return fmt.Errorf("kvstore error during Flush: %w", err)
	}

	return nil
}

func (d *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	batchedMutation, err := d.store.Batched()
	if err != nil {
		return nil, fmt.Errorf("kvstore error during Batch: %w", err)
	}

	return &Batch{
		batch: batchedMutation,
	}, nil
}

func (d *Datastore) Close() error {
	if !atomic.CompareAndSwapInt32(&d.status, 0, 1) {
		// already closed, or closing.
		d.wg.Wait()

		return nil
	}
	close(d.closing)
	d.wg.Wait()

	if err := d.store.Flush(); err != nil {
		return fmt.Errorf("kvstore error during Flush: %w", err)
	}

	if err := d.store.Close(); err != nil {
		return fmt.Errorf("kvstore error during Close: %w", err)
	}

	return nil
}

type Batch struct {
	batch kvstore.BatchedMutations
}

var _ ds.Batch = (*Batch)(nil)

// Put stores the object `value` named by `key`.
//
// The generalized Datastore interface does not impose a value type,
// allowing various datastore middleware implementations (which do not
// handle the values directly) to be composed together.
//
// Ultimately, the lowest-level datastore will need to do some value checking
// or risk getting incorrect values. It may also be useful to expose a more
// type-safe interface to your application, and do the checking up-front.
func (b *Batch) Put(ctx context.Context, key ds.Key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := b.batch.Set(key.Bytes(), value); err != nil {
		return fmt.Errorf("kvstore error during Set within batch: %w", err)
	}

	return nil
}

// Delete removes the value for given `key`. If the key is not in the
// datastore, this method returns no error.
func (b *Batch) Delete(ctx context.Context, key ds.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := b.batch.Delete(key.Bytes()); err != nil {
		return fmt.Errorf("kvstore error during Delete within batch: %w", err)
	}

	return nil
}

// Commit finalizes a transaction, attempting to commit it to the Datastore.
// May return an error if the transaction has gone stale. The presence of an
// error is an indication that the data was not committed to the Datastore.
func (b *Batch) Commit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := b.batch.Commit(); err != nil {
		return fmt.Errorf("kvstore error during Commit within batch: %w", err)
	}

	return nil
}
