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
	"github.com/pkg/errors"
	"github.com/streamnative/pulsarctl/pkg/cmdutils"
	ctlutils "github.com/streamnative/pulsarctl/pkg/ctl/utils"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

func SetRetentionCmd(vc *cmdutils.VerbCmd) {
	desc := cmdutils.LongDescription{}
	desc.CommandUsedFor = "Set the retention policy for a topic"
	desc.CommandPermission = "This command requires tenant admin permissions."

	var examples []cmdutils.Example
	removeRetention := cmdutils.Example{
		Desc:    "Set the retention policy for a topic",
		Command: "pulsarctl topics set-retention tenant/namespace/topic",
	}
	examples = append(examples, removeRetention)
	desc.CommandExamples = examples

	var out []cmdutils.Output
	successOut := cmdutils.Output{
		Desc: "normal output",
		Out:  "Set the retention policy for [topic] successfully",
	}

	noTopicName := cmdutils.Output{
		Desc: "you must specify a tenant/namespace/topic name, please check if the tenant/namespace/topic name is provided",
		Out:  "[✖]  the topic name is not specified or the topic name is specified more than one",
	}

	tenantNotExistError := cmdutils.Output{
		Desc: "the tenant does not exist",
		Out:  "[✖]  code: 404 reason: Tenant does not exist",
	}

	nsNotExistError := cmdutils.Output{
		Desc: "the namespace does not exist",
		Out:  "[✖]  code: 404 reason: Namespace (tenant/namespace) does not exist",
	}

	out = append(out, successOut, noTopicName, tenantNotExistError, nsNotExistError)
	desc.CommandOutput = out

	vc.SetDescription(
		"set-retention",
		"Set the retention policy for a topic",
		desc.ToString(),
		desc.ExampleToString(),
		"set-retention",
	)

	var timeStr string
	var sizeStr string

	vc.Command.Flags().StringVarP(&timeStr, "time", "", "",
		"Retention time in minutes (or minutes, hours, days, weeks eg: 100m, 3h, 2d, 5w). "+
			"0 means no retention and -1 means infinite time retention")
	vc.Command.Flags().StringVarP(&sizeStr, "size", "", "",
		"Retention size limit (eg: 10M, 16G, 3T). "+
			"0 or less than 1MB means no retention and -1 means infinite size retention")

	_ = vc.Command.MarkFlagRequired("time")
	_ = vc.Command.MarkFlagRequired("size")

	vc.SetRunFuncWithNameArg(func() error {
		return doSetRetention(vc, timeStr, sizeStr)
	}, "the topic name is not specified or the topic name is specified more than one")
}

func doSetRetention(vc *cmdutils.VerbCmd, timeStr string, sizeStr string) error {
	topic := vc.NameArg

	if timeStr == "" {
		return errors.New("missing option: --time")
	}
	if sizeStr == "" {
		return errors.New("missing option: --size")
	}

	duration, err := ctlutils.ParseRelativeTimeInSeconds(timeStr)
	if err != nil {
		return err
	}

	size, err := ctlutils.ValidateSizeString(sizeStr)
	if err != nil {
		return err
	}

	admin := cmdutils.NewPulsarClient()
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return err
	}

	err = admin.Topics().SetRetention(*topicName, utils.NewRetentionPolicies(int(duration.Seconds()), int(size)))
	if err == nil {
		vc.Command.Printf("Set the retention policy successfully on [%s]\n", topic)
	}

	return err
}
