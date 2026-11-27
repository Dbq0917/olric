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
	"testing"
	"time"

	"github.com/olric-data/olric/config"
	"github.com/olric-data/olric/internal/cluster/partitions"
	"github.com/olric-data/olric/internal/testcluster"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/olric-data/olric/pkg/storage"
	"github.com/stretchr/testify/require"
)

// errLastAccessEngine fails every GetLastAccess call while delegating the rest
// of the storage.Engine surface to the wrapped engine.
type errLastAccessEngine struct {
	storage.Engine
	err error
}

func (e *errLastAccessEngine) GetLastAccess(uint64) (int64, error) {
	return 0, e.err
}

func newDMapWithMaxIdle(t *testing.T) (*Service, *DMap) {
	t.Helper()

	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps = &config.DMaps{
		MaxIdleDuration: 100 * time.Millisecond,
		Engine:          config.NewEngine(),
	}
	require.NoError(t, c.DMaps.Sanitize())
	require.NoError(t, c.DMaps.Engine.Sanitize())

	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	t.Cleanup(cluster.Shutdown)

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)
	return s, dm
}

// A GetLastAccess failure must not be interpreted as a zero (ancient) last
// access time. That would make every key look idle and wipe the fragment in a
// single eviction sweep. The failing keys are skipped this round instead, while
// they still get evicted once the storage recovers and their real, stale last
// access times become readable again.
func TestDMap_Eviction_GetLastAccessFailureDoesNotEvictLiveKeys(t *testing.T) {
	s, dm := newDMapWithMaxIdle(t)

	// Create the target fragment up front and fill it directly through the
	// storage layer with enough victims for the eviction sampler, which stops
	// at maxKeyCount(20). Going through DMap.Put would hash most keys onto
	// other partitions because the test cluster only has seven partitions.
	firstHKey := partitions.HKey(dm.name, testutil.ToKey(0))
	part := dm.getPartitionByHKey(firstHKey, partitions.PRIMARY)
	target, err := dm.loadOrCreateFragment(part)
	require.NoError(t, err)

	target.Lock()
	for i := 0; i < 20; i++ {
		hkey := firstHKey + uint64(i) + 1

		entry := target.storage.NewEntry()
		entry.SetKey(testutil.ToKey(i))
		entry.SetValue(testutil.ToVal(i))
		entry.SetTimestamp(time.Now().UnixNano())
		entry.SetLastAccess(time.Now().UnixNano())
		require.NoError(t, target.storage.Put(hkey, entry))
	}
	target.Unlock()
	require.Equal(t, 20, target.storage.Stats().Length)

	readErr := errors.New("storage read failed")
	target.Lock()
	inner := target.storage
	target.storage = &errLastAccessEngine{Engine: inner, err: readErr}
	target.Unlock()

	// Even with an idle duration long exceeded, the read failure means the key
	// cannot be proven idle, so it must be treated as still alive.
	<-time.After(150 * time.Millisecond)

	target.Lock()
	idle := dm.isKeyIdleOnFragment(firstHKey+1, target)
	target.Unlock()
	require.False(t, idle, "GetLastAccess failure must not classify a key as idle")
	require.True(t, target.storage.Check(firstHKey+1),
		"the key must still be stored after the failed idle check")

	// A full eviction sweep must keep every key: TTLs are unset and every idle
	// decision goes through the failing read path on this fragment.
	before := target.storage.Stats().Length
	require.NotZero(t, before)
	s.scanFragmentForEviction(part.ID(), dm.name, target)
	require.Equal(t, before, target.storage.Stats().Length,
		"eviction sweep removed keys whose last access was unreadable")

	// Once the read succeeds again, the genuinely idle keys are evicted in the
	// next rounds: conservative skipping must not pin keys forever.
	target.Lock()
	target.storage = inner
	target.Unlock()

	recoveredLength := target.storage.Stats().Length

	// The janitor samples a window of keys per round and stops once a round
	// evicts fewer than 25% of a sample, so a single call need not evict
	// anything. Keep sweeping until a round proves the genuinely idle keys are
	// evictable again: conservative skipping must not pin them forever.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && target.storage.Stats().Length >= recoveredLength {
		s.scanFragmentForEviction(part.ID(), dm.name, target)
	}
	require.Less(t, target.storage.Stats().Length, recoveredLength,
		"idle keys were never evicted after the storage recovered")
}

// isKeyIdle is reached from the read path. It must answer conservatively
// instead of panicking when the fragment cannot be loaded, so one routing
// hiccup cannot crash the whole node.
func TestDMap_IsKeyIdle_MissingFragmentDoesNotPanic(t *testing.T) {
	_, dm := newDMapWithMaxIdle(t)

	hkey := partitions.HKey(dm.name, "missing-key")
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)

	// No fragment exists for this dmap on the partition: loadFragment returns
	// errFragmentNotFound, the only loadFragment failure in practice.
	require.NotPanics(t, func() {
		require.False(t, dm.isKeyIdle(hkey))
	})

	_, err := dm.loadFragment(part)
	require.ErrorIs(t, err, errFragmentNotFound)
}
