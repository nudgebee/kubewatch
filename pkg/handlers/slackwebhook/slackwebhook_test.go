/*
Copyright 2018 Bitnami

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package slackwebhook

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/bitnami-labs/kubewatch/config"
)

// Slack incoming webhooks bind a channel and a username to the URL itself,
// so Init only requires the URL — channel/username/emoji are optional
// per-message overrides.
func TestWebhookInit(t *testing.T) {
	t.Setenv("KW_SLACK_CHANNEL", "")
	t.Setenv("KW_SLACK_USERNAME", "")
	t.Setenv("KW_SLACK_EMOJI", "")
	t.Setenv("KW_SLACK_WEBHOOK_URL", "")

	missingURL := fmt.Errorf(webhookErrMsg, "Missing Slack Webhook url")

	tests := []struct {
		name string
		cfg  config.SlackWebhook
		err  error
	}{
		{"all fields", config.SlackWebhook{Channel: "foo", Username: "bar", Slackwebhookurl: "you"}, nil},
		{"only url", config.SlackWebhook{Slackwebhookurl: "you"}, nil},
		{"only channel", config.SlackWebhook{Channel: "foo"}, missingURL},
		{"only username", config.SlackWebhook{Username: "bar"}, missingURL},
		{"only emoji", config.SlackWebhook{Emoji: ":kubernetes:"}, missingURL},
		{"empty", config.SlackWebhook{}, missingURL},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SlackWebhook{}
			c := &config.Config{}
			c.Handler.SlackWebhook = tt.cfg
			if err := s.Init(c); !reflect.DeepEqual(err, tt.err) {
				t.Fatalf("Init() = %v, want %v", err, tt.err)
			}
		})
	}
}
