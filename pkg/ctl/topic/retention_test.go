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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/streamnative/pulsarctl/pkg/test"

	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
	"github.com/stretchr/testify/assert"
)

func TestRetention(t *testing.T) {
	topicName := fmt.Sprintf("test-retention-topic-%s", test.RandomSuffix())
	_, execErr, _, _ := TestTopicCommands(CreateTopicCmd, []string{"create", topicName, "1"})
	assert.Nil(t, execErr)

	setArgs := []string{"set-retention", topicName, "--time", "1h", "--size", "10m"}
	out, execErr, nameErr, cmdErr := TestTopicCommands(SetRetentionCmd, setArgs)
	assert.Nil(t, execErr)
	assert.Nil(t, nameErr)
	assert.Nil(t, cmdErr)
	assert.NotNil(t, out)
	assert.NotEmpty(t, out.String())

	// waiting for the pulsar to be configured
	time.Sleep(time.Second)

	getArgs := []string{"get-retention", topicName}
	out, execErr, nameErr, cmdErr = TestTopicCommands(GetRetentionCmd, getArgs)
	assert.Nil(t, execErr)
	assert.Nil(t, nameErr)
	assert.Nil(t, cmdErr)
	assert.NotNil(t, out)
	assert.NotEmpty(t, out.String())

	var data utils.RetentionPolicies
	err := json.Unmarshal(out.Bytes(), &data)
	assert.Nil(t, err)
	assert.Equal(t, 3600, data.RetentionTimeInMinutes)
	assert.Equal(t, int64(10*1024*1024), data.RetentionSizeInMB)

	removeArgs := []string{"remove-retention", topicName}
	out, execErr, nameErr, cmdErr = TestTopicCommands(RemoveRetentionCmd, removeArgs)
	assert.Nil(t, execErr)
	assert.Nil(t, nameErr)
	assert.Nil(t, cmdErr)
	assert.NotNil(t, out)
	assert.NotEmpty(t, out.String())

	out, execErr, nameErr, cmdErr = TestTopicCommands(GetRetentionCmd, getArgs)
	assert.Nil(t, execErr)
	assert.Nil(t, nameErr)
	assert.Nil(t, cmdErr)
	assert.NotNil(t, out)
	assert.NotEmpty(t, out.String())

	data = utils.RetentionPolicies{}
	err = json.Unmarshal(out.Bytes(), &data)
	assert.Nil(t, err)
	assert.Equal(t, 0, data.RetentionTimeInMinutes)
	assert.Equal(t, int64(0), data.RetentionSizeInMB)
}
