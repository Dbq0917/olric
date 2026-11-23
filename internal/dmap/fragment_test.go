// Copyright 2018-2026 The Olric Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dmap

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/discovery"
	"github.com/olric-data/olric/internal/protocol"
	"github.com/olric-data/olric/internal/testcluster"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestDMap_Fragment(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	t.Run("loadFragment", func(t *testing.T) {
		part := s.primary.PartitionByID(1)
		_, err = dm.loadFragment(part)
		if !errors.Is(err, errFragmentNotFound) {
			t.Fatalf("Expected %v. Got: %v", errFragmentNotFound, err)
		}
	})

	t.Run("newFragment", func(t *testing.T) {
		_, err := dm.newFragment()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	t.Run("loadFragment -- errFragmentNotFound", func(t *testing.T) {
		part := dm.getPartitionByHKey(123, partitions.PRIMARY)
		_, err := dm.loadFragment(part)
		if !errors.Is(err, errFragmentNotFound) {
			t.Fatalf("Expected %v. Got: %v", errFragmentNotFound, err)
		}
	})

	t.Run("loadOrCreateFragment", func(t *testing.T) {
		part := dm.getPartitionByHKey(123, partitions.PRIMARY)
		_, err = dm.loadOrCreateFragment(part)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		_, err := dm.loadFragment(part)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
}

func TestDMap_Fragment_Concurrent_Access(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	part := dm.getPartitionByHKey(123, partitions.PRIMARY)

	var mtx sync.RWMutex
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			f, err := dm.loadOrCreateFragment(part)
			if err != nil {
				t.Errorf("Expected nil. Got: %v", err)
			}

			e := f.storage.NewEntry()
			e.SetKey(testutil.ToKey(idx))

			mtx.Lock()
			// storage engine is not thread-safe
			err = f.storage.Put(uint64(idx), e)
			mtx.Unlock()

			if err != nil {
				t.Errorf("Expected nil. Got: %v", err)
			}
		}(i)
	}

	wg.Wait()

	f, err := dm.loadFragment(part)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 1000; i++ {
		entry, err := f.storage.Get(uint64(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if entry.Key() != testutil.ToKey(i) {
			t.Fatalf("Expected key: %s. Got: %s", testutil.ToKey(i), entry.Key())
		}
	}
}

func TestDMap_Fragment_MoveDoesNotBlockReadsDuringNetworkTransfer(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	hkey := uint64(123)
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadOrCreateFragment(part)
	require.NoError(t, err)

	entry := f.storage.NewEntry()
	entry.SetKey("my-key")
	entry.SetValue([]byte("my-value"))
	f.Lock()
	require.NoError(t, f.storage.Put(hkey, entry))
	f.Unlock()

	port, err := testutil.GetFreePort()
	require.NoError(t, err)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	defer ln.Close()

	received := make(chan struct{})
	release := make(chan struct{})

	go func() {
		_ = redcon.Serve(ln, func(conn redcon.Conn, cmd redcon.Command) {
			if string(cmd.Args[0]) == protocol.Internal.MoveFragment {
				close(received)
				<-release
				conn.WriteString(protocol.StatusOK)
				return
			}
			conn.WriteError("unexpected command")
		}, nil, nil)
	}()

	moveErr := make(chan error, 1)
	go func() {
		moveErr <- f.Move(part, "dmap.mydmap", []discovery.Member{{Name: addr}})
	}()

	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for migration request")
	}

	readLockAcquired := make(chan struct{})
	go func() {
		f.RLock()
		defer f.RUnlock()
		close(readLockAcquired)
	}()

	select {
	case <-readLockAcquired:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("fragment reads are blocked while migration is in flight")
	}

	close(release)
	select {
	case err := <-moveErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Move to finish")
	}
}
