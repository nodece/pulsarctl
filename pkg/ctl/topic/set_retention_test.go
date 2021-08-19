// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package topic

import (
	"fmt"
	"testing"

	"github.com/streamnative/pulsarctl/pkg/test"
	"github.com/stretchr/testify/assert"
)

func TestSetRetentionArgError(t *testing.T) {
	args := []string{"set-retention"}
	_, execErr, _, err := TestTopicCommands(SetRetentionCmd, args)
	assert.Nil(t, execErr)
	assert.EqualError(t, err, "required flag(s) \"size\", \"time\" not set")

	args = []string{"set-retention", "--time", "100"}
	_, execErr, _, err = TestTopicCommands(SetRetentionCmd, args)
	assert.Nil(t, execErr)
	assert.EqualError(t, err, "required flag(s) \"size\" not set")

	args = []string{"set-retention", "--time", "100", "--size", "200"}
	_, execErr, _, err = TestTopicCommands(SetRetentionCmd, args)
	assert.NotNil(t, execErr)
	assert.Nil(t, err)
}

func TestSetRetentionCmd(t *testing.T) {
	topic := fmt.Sprintf("test-set-retention-topic-%s", test.RandomSuffix())

	args := []string{"set-retention", topic, "--time", "1h", "--size", "100m"}
	out, execErr, nameErr, cmdErr := TestTopicCommands(SetRetentionCmd, args)

	assert.Nil(t, execErr)
	assert.Nil(t, nameErr)
	assert.Nil(t, cmdErr)
	assert.NotNil(t, out)
	assert.NotEmpty(t, out.String())

	t.Log(out.String())
}
