package etcd

import (
	estore "github.com/gly-hub/grpc-etcd/store"
	"github.com/gly-hub/libkv"
	"github.com/gly-hub/libkv/store"
	"github.com/gly-hub/libkv/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	client = "localhost:4001"
)

func makeEtcdClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			Username:          "test",
			Password:          "very-secure",
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestEtcdRegister(t *testing.T) {
	Register()

	kv, err := libkv.NewStore(estore.ETCD, []string{client}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Etcd); !ok {
		t.Fatal("Error registering and initializing etcd")
	}
}

func TestEtcdStore(t *testing.T) {
	kv := makeEtcdClient(t)
	lockKV := makeEtcdClient(t)
	ttlKV := makeEtcdClient(t)

	defer testutils.RunCleanup(t, kv)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockKV)
	testutils.RunTestLockWait(t, kv, lockKV)
	testutils.RunTestTTL(t, kv, ttlKV)
}
