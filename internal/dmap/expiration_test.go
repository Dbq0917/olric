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
	"context"
	"testing"
	"time"

	"github.com/olric-data/olric/internal/testcluster"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestExpirationQueue_OrdersAndDeduplicates(t *testing.T) {
	q := newExpirationQueue()
	dm := &DMap{name: "test"}

	q.schedule(dm, 3, 300)
	q.schedule(dm, 1, 100)
	q.schedule(dm, 2, 200)
	q.schedule(dm, 2, 50)

	due, next := q.popDue(40, 10)
	require.Empty(t, due)
	require.Equal(t, int64(50), next)

	due, next = q.popDue(50, 10)
	require.Len(t, due, 1)
	require.Equal(t, uint64(2), due[0].hkey)
	require.Equal(t, int64(100), next)

	q.remove(dm, 1)
	due, next = q.popDue(300, 10)
	require.Len(t, due, 1)
	require.Equal(t, uint64(3), due[0].hkey)
	require.Zero(t, next)
}

func TestDMap_Expiration_ReclaimsKeysWithoutScanner(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("expiration.test")
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 500; i++ {
		key := testutil.ToKey(i)
		require.NoError(t, dm.Put(ctx, key, testutil.ToVal(i), &PutConfig{
			HasPX: true,
			PX:    50 * time.Millisecond,
		}))
	}

	require.Eventually(t, func() bool {
		return primaryStorageLength(s) == 0
	}, 3*time.Second, 10*time.Millisecond)
}

func TestDMap_Expiration_RenewalAndOverwrite(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("expiration.test")
	require.NoError(t, err)

	ctx := context.Background()
	key := testutil.ToKey(1)
	require.NoError(t, dm.Put(ctx, key, testutil.ToVal(1), &PutConfig{
		HasPX: true,
		PX:    50 * time.Millisecond,
	}))
	require.NoError(t, dm.Put(ctx, key, testutil.ToVal(2), &PutConfig{
		HasPX: true,
		PX:    time.Second,
	}))

	time.Sleep(100 * time.Millisecond)
	_, err = dm.Get(ctx, key)
	require.NoError(t, err, "renewed key was reclaimed at its old deadline")

	require.NoError(t, dm.Put(ctx, key, testutil.ToVal(3), nil))
	time.Sleep(100 * time.Millisecond)
	value, err := dm.Get(ctx, key)
	require.NoError(t, err, "key overwritten without a TTL was reclaimed")
	require.Equal(t, testutil.ToVal(3), value.Value())
}

func primaryStorageLength(s *Service) (length int) {
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := s.primary.PartitionByID(partID)
		part.Map().Range(func(_, value interface{}) bool {
			f := value.(*fragment)
			length += f.Stats().Length
			return true
		})
	}
	return length
}

func TestExpirationQueue_RemoveDMap(t *testing.T) {
	q := newExpirationQueue()
	dm1 := &DMap{name: "one"}
	dm2 := &DMap{name: "two"}

	q.schedule(dm1, 1, 100)
	q.schedule(dm1, 2, 200)
	q.schedule(dm2, 3, 300)
	q.removeDMap(dm1)

	due, next := q.popDue(300, 10)
	require.Len(t, due, 1)
	require.Equal(t, uint64(3), due[0].hkey)
	require.Zero(t, next)
}
