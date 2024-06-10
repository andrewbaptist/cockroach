// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestPulsarParams tests the validity of pulsar sink parameters.
func TestPulsarParams(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t)
	defer stopServer()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

	for _, tc := range []struct {
		name          string
		uri           string
		expectedError string
	}{
		{
			name:          "topic_prefix",
			uri:           "pulsar://nope/?topic_prefix=foo",
			expectedError: "topic_prefix is not yet supported",
		},
		{
			name:          "topic_name",
			uri:           "pulsar://nope/?topic_name=foo",
			expectedError: "topic_name is not yet supported",
		},
		{
			name:          "schema_topic",
			uri:           "pulsar://nope/?schema_topic=foo",
			expectedError: "schema_topic is not yet supported",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, tc.expectedError,
				fmt.Sprintf(`CREATE CHANGEFEED FOR foo INTO '%s'`, tc.uri),
			)
		})
	}
}
