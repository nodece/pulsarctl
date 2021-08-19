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

func TestRemoveRetentionArgError(t *testing.T) {
	args := []string{"remove-retention"}
	_, execErr, nameErr, cmdErr := TestTopicCommands(RemoveRetentionCmd, args)

	assert.NotNil(t, execErr)
	assert.NotNil(t, nameErr)
	assert.Nil(t, cmdErr)
	assert.Equal(t, "the topic name is not specified or the topic name is specified more than one", nameErr.Error())
}

func TestRemoveRetentionCmd(t *testing.T) {
	topic := fmt.Sprintf("test-remove-retention-topic-%s", test.RandomSuffix())

	args := []string{"remove-retention", topic}
	out, execErr, nameErr, cmdErr := TestTopicCommands(RemoveRetentionCmd, args)

	assert.Nil(t, execErr)
	assert.Nil(t, nameErr)
	assert.Nil(t, cmdErr)
	assert.NotNil(t, out)
	assert.NotEmpty(t, out.String())

	t.Log(out.String())
}
