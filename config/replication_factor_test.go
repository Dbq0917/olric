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

package config

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplicationFactorConfig(t *testing.T) {
	c := New("local")
	require.Equal(t, DefaultReplicationFactor, c.ReplicationFactor)
}

func TestReplicationFactorConfigLoad(t *testing.T) {
	raw := []byte(`
server:
  bindAddr: "127.0.0.1"
  bindPort: 3320
  replicationFactor: 3
memberlist:
  environment: "local"
  bindAddr: "127.0.0.1"
  bindPort: 3322
`)

	f := createTmpFile(t, "olric-replication-factor-test")
	_, err := f.Write(bytes.NewBuffer(raw).Bytes())
	require.NoError(t, err)

	c, err := Load(f.Name())
	require.NoError(t, err)
	require.Equal(t, 3, c.ReplicationFactor)
}

func TestReplicationFactorConfigValidation(t *testing.T) {
	c := New("local")
	c.ReplicationFactor = -1
	require.Error(t, c.Validate())
}
