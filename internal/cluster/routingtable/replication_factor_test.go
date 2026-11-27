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

package routingtable

import (
	"errors"
	"testing"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/olric-data/olric/internal/discovery"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/olric-data/olric/internal/testutil/mockfragment"
	"github.com/stretchr/testify/require"
)

func TestRoutingTableReconcileReplicationFactor(t *testing.T) {
	c := testutil.NewConfig()
	c.ReplicationFactor = 20

	members := []consistent.Member{
		discovery.Member{Name: "127.0.0.1:3320", ID: 1},
		discovery.Member{Name: "127.0.0.1:3321", ID: 2},
		discovery.Member{Name: "127.0.0.1:3322", ID: 3},
	}

	rt := &RoutingTable{
		config:            c,
		consistent:        newConsistent(c),
		replicationFactor: c.ReplicationFactor,
	}
	for _, member := range members {
		rt.consistentAdd(member.(discovery.Member))
	}

	c.ReplicationFactor = 1
	rt.reconcileConsistent()
	require.Equal(t, 1, rt.replicationFactor)
	require.Len(t, rt.consistent.GetMembers(), len(members))

	expected := consistent.New(members, consistent.Config{
		Hasher:            c.Hasher,
		PartitionCount:    int(c.PartitionCount),
		ReplicationFactor: c.ReplicationFactor,
		Load:              c.LoadFactor,
	})
	for partID := uint64(0); partID < c.PartitionCount; partID++ {
		require.Equal(t, expected.GetPartitionOwner(int(partID)).String(),
			rt.partitionOwner(partID).String())
	}
}

func TestRoutingTableReplicationFactorChangePreservesPreviousOwners(t *testing.T) {
	cluster := newTestCluster()
	defer cluster.cancel()

	c1 := testutil.NewConfig()
	c1.ReplicationFactor = 20
	rt1, err := cluster.addNode(c1)
	require.NoError(t, err)

	c2 := testutil.NewConfig()
	c2.ReplicationFactor = 20
	rt2, err := cluster.addNode(c2)
	require.NoError(t, err)

	err = testutil.TryWithInterval(10, 100*time.Millisecond, func() error {
		if !rt2.IsBootstrapped() {
			return errors.New("the second node cannot be bootstrapped")
		}
		return nil
	})
	require.NoError(t, err)

	previousOwners := make(map[uint64]discovery.Member)
	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		part := rt1.primary.PartitionByID(partID)
		owner := part.Owner()
		previousOwners[partID] = owner

		target := rt1
		if !owner.CompareByID(rt1.This()) {
			target = rt2
		}
		fragment := mockfragment.New()
		fragment.Put("test-key", "test-value")
		target.primary.PartitionByID(partID).Map().Store("test-data", fragment)
	}

	c1.ReplicationFactor = 1
	rt1.UpdateEagerly()

	expected := consistent.New([]consistent.Member{rt1.This(), rt2.This()}, consistent.Config{
		Hasher:            c1.Hasher,
		PartitionCount:    int(c1.PartitionCount),
		ReplicationFactor: c1.ReplicationFactor,
		Load:              c1.LoadFactor,
	})
	for partID := uint64(0); partID < c1.PartitionCount; partID++ {
		part := rt1.primary.PartitionByID(partID)
		newOwner := expected.GetPartitionOwner(int(partID)).(discovery.Member)
		require.True(t, part.Owner().CompareByID(newOwner))

		owners := part.Owners()
		previousOwner := previousOwners[partID]
		if previousOwner.CompareByID(newOwner) {
			require.Len(t, owners, 1)
			continue
		}
		require.Len(t, owners, 2)
		require.True(t, owners[0].CompareByID(previousOwner))
		require.True(t, owners[1].CompareByID(newOwner))
	}
}
