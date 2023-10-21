package etcd

import (
	"context"
	"crypto/tls"
	"errors"
	estore "github.com/gly-hub/grpc-etcd/store"
	"github.com/gly-hub/libkv"
	"github.com/gly-hub/libkv/store"
	etcd "go.etcd.io/etcd/client/v2"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

var (
	// ErrAbortTryLock 在用户停止尝试通过向stop chan发送信号
	// 来寻求锁时抛出，这用于验证操作是否成功
	ErrAbortTryLock = errors.New("lock operation aborted")
)

type Etcd struct {
	client etcd.KeysAPI
}

type etcdLock struct {
	client etcd.KeysAPI
	key    string
	value  string
	ttl    time.Duration

	// 当调用者想要停止更新锁时关闭。
	// 可以直接调用Unlock()方法
	stopRenew chan struct{}
	// 当锁被持有时，这是键的最后修改索引。
	// 用于在延长锁TTL时进行条件更新，以及在
	// 调用Unlock()时进行条件删除。
	lastIndex uint64
	// 当锁被持有时，这个函数将取消被锁定的上下文。
	// 这是由Unlock()方法调用的，目的是为了停止后
	// 台持有线程程，同时在后台持有线程程中延迟调用，
	// 以防锁由于错误或stopRenew通道被关闭而丢失。
	// 调用这个函数也会关闭Lock()方法返回的chan。
	cancel context.CancelFunc
	// 用于将Unlock()调用与后台保持例程同步。当后台
	// 例程退出时，该通道关闭，表明可以有条件地删除键
	doneHolding chan struct{}
}

const (
	periodicSync      = 5 * time.Minute
	defaultLockTTL    = 20 * time.Second
	defaultUpdateTime = 5 * time.Second
)

func Register() {
	libkv.AddStore(estore.ETCD, New)
}

func New(addrs []string, options *store.Config) (store.Store, error) {
	s := &Etcd{}
	var (
		entries []string
		err     error
	)

	entries = store.CreateEndpoints(addrs, "http")
	cfg := &etcd.Config{
		Endpoints:               entries,
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: 3 * time.Second,
	}

	if options != nil {
		if options.TLS != nil {
			setTLS(cfg, options.TLS, addrs)
		}
		if options.ConnectionTimeout != 0 {
			setTimeout(cfg, options.ConnectionTimeout)
		}
		if options.Username != "" {
			setCredentials(cfg, options.Username, options.Password)
		}
	}
	var c etcd.Client
	if c, err = etcd.New(*cfg); err != nil {
		log.Fatal(err)
	}

	s.client = etcd.NewKeysAPI(c)

	go func() {
		for {
			if err = c.AutoSync(context.Background(), periodicSync); err != nil {
				return
			}
		}
	}()

	return s, nil
}

func setTLS(cfg *etcd.Config, tls *tls.Config, addrs []string) {
	cfg.Endpoints = store.CreateEndpoints(addrs, "https")

	cfg.Transport = &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tls,
	}
}

func setTimeout(cfg *etcd.Config, time time.Duration) {
	cfg.HeaderTimeoutPerRequest = time
}

func setCredentials(cfg *etcd.Config, username, password string) {
	cfg.Username = username
	cfg.Password = password
}

// normalize 规范化键以便在Etcd中使用
func (s *Etcd) normalize(key string) string {
	key = store.Normalize(key)
	return strings.TrimPrefix(key, "/")
}

// keyNotFound 检查keyapi返回的错误，
// 以验证密钥是否存在于存储中
func keyNotFound(err error) bool {
	if err != nil {
		if etcdError, ok := err.(etcd.Error); ok {
			if etcdError.Code == etcd.ErrorCodeKeyNotFound ||
				etcdError.Code == etcd.ErrorCodeNotFile ||
				etcdError.Code == etcd.ErrorCodeNotDir {
				return true
			}
		}
	}
	return false
}

// Get 获取值
func (s *Etcd) Get(key string) (pair *store.KVPair, err error) {
	getOpts := &etcd.GetOptions{
		Quorum: true,
	}
	var result *etcd.Response
	if result, err = s.client.Get(context.Background(), s.normalize(key), getOpts); err != nil {
		if keyNotFound(err) {
			err = store.ErrKeyNotFound
			return
		}
		return nil, err
	}

	pair = &store.KVPair{
		Key:       key,
		Value:     []byte(result.Node.Value),
		LastIndex: result.Node.ModifiedIndex,
	}
	return pair, nil
}

// Put 存储key值
func (s *Etcd) Put(key string, value []byte, opts *store.WriteOptions) error {
	setOpts := &etcd.SetOptions{}

	if opts != nil {
		setOpts.Dir = opts.IsDir
		setOpts.TTL = opts.TTL
	}

	_, err := s.client.Set(context.Background(), s.normalize(key), string(value), setOpts)
	return err
}

// Delete 删除key值
func (s *Etcd) Delete(key string) error {
	opts := &etcd.DeleteOptions{Recursive: false}

	_, err := s.client.Delete(context.Background(), s.normalize(key), opts)
	if keyNotFound(err) {
		return store.ErrKeyNotFound
	}
	return err
}

// Exists 校验key是否存在
func (s *Etcd) Exists(key string) (bool, error) {
	_, err := s.Get(key)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Watch 监听key变更。它返回一个通道，该通道将接收
// 更改或传递错误。创建后，将首先将当前值发送到通道。
// 提供一个非nil stopCh可以用来停止监听。
func (s *Etcd) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	opts := &etcd.WatcherOptions{Recursive: true}
	watcher := s.client.Watcher(s.normalize(key), opts)

	watchCh := make(chan *store.KVPair)

	go func() {
		defer close(watchCh)

		pair, err := s.Get(key)
		if err != nil {
			return
		}
		watchCh <- pair

		for {
			select {
			case <-stopCh:
				return
			default:
			}
			var result *etcd.Response
			result, err = watcher.Next(context.Background())
			if err != nil {
				return
			}
			watchCh <- &store.KVPair{
				Key:       key,
				Value:     []byte(result.Node.Value),
				LastIndex: result.Node.ModifiedIndex,
			}
		}
	}()

	return watchCh, nil
}

// WatchTree 监视“目录”上的更改，它返回一个通道，该通道将接收更改或传递错误。
// 创建一个监听后，当前的子值将被发送到通道。
// 提供一个非nil stopCh可以用来停止监听。
func (s *Etcd) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	watchOpts := &etcd.WatcherOptions{Recursive: true}
	watcher := s.client.Watcher(s.normalize(directory), watchOpts)

	watchCh := make(chan []*store.KVPair)

	go func() {
		defer close(watchCh)
		// 获取子节点
		list, err := s.List(directory)
		if err != nil {
			return
		}
		watchCh <- list

		for {
			select {
			case <-stopCh:
				return
			default:
			}

			_, err = watcher.Next(context.Background())
			if err != nil {
				return
			}
			list, err = s.List(directory)
			if err != nil {
				return
			}
			watchCh <- list
		}
	}()
	return watchCh, nil
}

// AtomicPut 如果键在此期间没有被修改，AtomicPut
// 将在“key”处放置一个值
func (s *Etcd) AtomicPut(key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (
	bool, *store.KVPair, error) {
	var (
		meta *etcd.Response
		err  error
	)
	setOpts := &etcd.SetOptions{}

	if previous != nil {
		setOpts.PrevExist = etcd.PrevNoExist
		setOpts.PrevIndex = previous.LastIndex
		if previous.Value != nil {
			setOpts.PrevValue = string(previous.Value)
		}
	} else {
		setOpts.PrevExist = etcd.PrevNoExist
	}

	if opts != nil {
		if opts.TTL > 0 {
			setOpts.TTL = opts.TTL
		}
	}

	meta, err = s.client.Set(context.Background(), s.normalize(key), string(value), setOpts)
	if err != nil {
		if etcdErr, ok := err.(etcd.Error); ok {
			// 对比失败
			if etcdErr.Code == etcd.ErrorCodeTestFailed {
				return false, nil, store.ErrKeyModified
			}
			// 节点错误
			if etcdErr.Code == etcd.ErrorCodeNodeExist {
				return false, nil, store.ErrKeyExists
			}
		}
		return false, nil, err
	}
	updated := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: meta.Node.ModifiedIndex,
	}

	return true, updated, nil
}

// AtomicDelete 如果在此期间键未被修改，AtomicDelete将删除“key”处的值
func (s *Etcd) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if previous == nil {
		return false, store.ErrPreviousNotSpecified
	}

	delOpts := &etcd.DeleteOptions{}

	if previous != nil {
		delOpts.PrevIndex = previous.LastIndex
		if previous.Value != nil {
			delOpts.PrevValue = string(previous.Value)
		}
	}

	_, err := s.client.Delete(context.Background(), s.normalize(key), delOpts)
	if err != nil {
		if etcdError, ok := err.(etcd.Error); ok {
			// Key Not Found
			if etcdError.Code == etcd.ErrorCodeKeyNotFound {
				return false, store.ErrKeyNotFound
			}
			// Compare failed
			if etcdError.Code == etcd.ErrorCodeTestFailed {
				return false, store.ErrKeyModified
			}
		}
		return false, err
	}

	return true, nil
}

// List 获取给定目录子节点
func (s *Etcd) List(directory string) ([]*store.KVPair, error) {
	getOpts := &etcd.GetOptions{
		Quorum:    true,
		Recursive: true,
		Sort:      true,
	}

	resp, err := s.client.Get(context.Background(), s.normalize(directory), getOpts)
	if err != nil {
		if keyNotFound(err) {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	var kv []*store.KVPair
	for _, n := range resp.Node.Nodes {
		kv = append(kv, &store.KVPair{
			Key:       n.Key,
			Value:     []byte(n.Value),
			LastIndex: n.ModifiedIndex,
		})
	}
	return kv, nil
}

// DeleteTree 删除给定目录下的一系列键
func (s *Etcd) DeleteTree(directory string) error {
	delOpts := &etcd.DeleteOptions{
		Recursive: true,
	}

	_, err := s.client.Delete(context.Background(), s.normalize(directory), delOpts)
	if keyNotFound(err) {
		return store.ErrKeyNotFound
	}
	return err
}

func (s *Etcd) NewLock(key string, opts *store.LockOptions) (store.Locker, error) {
	var value string
	ttl := defaultLockTTL
	renewCh := make(chan struct{})

	if opts != nil {
		if opts.Value != nil {
			value = string(opts.Value)
		}
		if opts.TTL != 0 {
			ttl = opts.TTL
		}
		if opts.RenewLock != nil {
			renewCh = opts.RenewLock
		}
	}

	lock := &etcdLock{
		client:    s.client,
		key:       s.normalize(key),
		value:     value,
		ttl:       ttl,
		stopRenew: renewCh,
	}
	return lock, nil
}

func (l *etcdLock) Lock(stopChan chan struct{}) (<-chan struct{}, error) {
	setOpts := &etcd.SetOptions{
		TTL:       l.ttl,
		PrevExist: etcd.PrevNoExist,
	}

	for {
		resp, err := l.client.Set(context.Background(), l.key, l.value, setOpts)
		if err == nil {
			l.lastIndex = resp.Node.ModifiedIndex
			lockedCtx, cancel := context.WithCancel(context.Background())
			l.cancel = cancel
			l.doneHolding = make(chan struct{})

			go l.holdLock(lockedCtx)
			return lockedCtx.Done(), nil
		}
		etcdErr, ok := err.(etcd.Error)
		if !ok || etcdErr.Code != etcd.ErrorCodeNodeExist {
			return nil, err
		}

		if err = l.waitLock(stopChan, etcdErr.Index); err != nil {
			return nil, err
		}
	}
}

func (l *etcdLock) holdLock(ctx context.Context) {
	defer close(l.doneHolding)
	defer l.cancel()

	update := time.NewTicker(l.ttl / 3)
	defer update.Stop()

	setOpts := &etcd.SetOptions{TTL: l.ttl}

	for {
		select {
		case <-update.C:
			setOpts.PrevIndex = l.lastIndex
			resp, err := l.client.Set(ctx, l.key, l.value, setOpts)
			if err != nil {
				return
			}
			l.lastIndex = resp.Node.ModifiedIndex
		case <-l.stopRenew:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (l *etcdLock) waitLock(stopWait <-chan struct{}, afterIndex uint64) error {
	waitCtx, waitCancel := context.WithCancel(context.Background())
	defer waitCancel()
	go func() {
		select {
		case <-stopWait:
			waitCancel()
		case <-waitCtx.Done():
		}
	}()

	watcher := l.client.Watcher(l.key, &etcd.WatcherOptions{AfterIndex: afterIndex})
	for {
		event, err := watcher.Next(waitCtx)
		if err != nil {
			if err == context.Canceled {
				return ErrAbortTryLock
			}
			return err
		}
		switch event.Action {
		case "delete", "compareAndDelete", "expire":
			return nil
		}
	}
}

func (l *etcdLock) Unlock() error {
	l.cancel()
	<-l.doneHolding

	var err error
	if l.lastIndex != 0 {
		delOpts := &etcd.DeleteOptions{
			PrevIndex: l.lastIndex,
		}
		_, err = l.client.Delete(context.Background(), l.key, delOpts)
	}
	return err
}

func (s *Etcd) Close() {
	return
}
