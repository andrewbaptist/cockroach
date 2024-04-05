// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Code generated by generate-logictest, DO NOT EDIT.

package testlocal_read_committed

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/logictest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const configIdx = 3

var cclLogicTestDir string
var execBuildLogicTestDir string

func init() {
	if bazel.BuiltWithBazel() {
		var err error
		cclLogicTestDir, err = bazel.Runfile("pkg/ccl/logictestccl/testdata/logic_test")
		if err != nil {
			panic(err)
		}
	} else {
		cclLogicTestDir = "../../../../ccl/logictestccl/testdata/logic_test"
	}
	if bazel.BuiltWithBazel() {
		var err error
		execBuildLogicTestDir, err = bazel.Runfile("pkg/sql/opt/exec/execbuilder/testdata")
		if err != nil {
			panic(err)
		}
	} else {
		execBuildLogicTestDir = "../../../../sql/opt/exec/execbuilder/testdata"
	}
}

func TestMain(m *testing.M) {
	defer ccl.TestingEnableEnterprise()()
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)

	defer serverutils.TestingSetDefaultTenantSelectionOverride(
		base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(76378),
	)()

	os.Exit(m.Run())
}

func runCCLLogicTest(t *testing.T, file string) {
	skip.UnderDeadlock(t, "times out and/or hangs")
	logictest.RunLogicTest(t, logictest.TestServerArgs{}, configIdx, filepath.Join(cclLogicTestDir, file))
}
func runExecBuildLogicTest(t *testing.T, file string) {
	defer sql.TestingOverrideExplainEnvVersion("CockroachDB execbuilder test version")()
	skip.UnderDeadlock(t, "times out and/or hangs")
	serverArgs := logictest.TestServerArgs{
		DisableWorkmemRandomization: true,
		// Disable the direct scans in order to keep the output of EXPLAIN (VEC)
		// deterministic.
		DisableDirectColumnarScans: true,
	}
	logictest.RunLogicTest(t, serverArgs, configIdx, filepath.Join(execBuildLogicTestDir, file))
}

// TestLogic_tmp runs any tests that are prefixed with "_", in which a dedicated
// test is not generated for. This allows developers to create and run temporary
// test files that are not checked into the repository, without repeatedly
// regenerating and reverting changes to this file, generated_test.go.
//
// TODO(mgartner): Add file filtering so that individual files can be run,
// instead of all files with the "_" prefix.
func TestLogic_tmp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var glob string
	glob = filepath.Join(cclLogicTestDir, "_*")
	logictest.RunLogicTests(t, logictest.TestServerArgs{}, configIdx, glob)
	glob = filepath.Join(execBuildLogicTestDir, "_*")
	serverArgs := logictest.TestServerArgs{
		DisableWorkmemRandomization: true,
	}
	logictest.RunLogicTests(t, serverArgs, configIdx, glob)
}

func TestReadCommittedLogicCCL_fips_ready(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "fips_ready")
}

func TestReadCommittedLogicCCL_fk_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "fk_read_committed")
}

func TestReadCommittedLogicCCL_hash_sharded_index_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "hash_sharded_index_read_committed")
}

func TestReadCommittedLogicCCL_nested_routines(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "nested_routines")
}

func TestReadCommittedLogicCCL_new_schema_changer(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "new_schema_changer")
}

func TestReadCommittedLogicCCL_partitioning_enum(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "partitioning_enum")
}

func TestReadCommittedLogicCCL_pgcrypto_builtins(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "pgcrypto_builtins")
}

func TestReadCommittedLogicCCL_plpgsql_block(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "plpgsql_block")
}

func TestReadCommittedLogicCCL_plpgsql_call(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "plpgsql_call")
}

func TestReadCommittedLogicCCL_plpgsql_cursor(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "plpgsql_cursor")
}

func TestReadCommittedLogicCCL_plpgsql_into(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "plpgsql_into")
}

func TestReadCommittedLogicCCL_plpgsql_record(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "plpgsql_record")
}

func TestReadCommittedLogicCCL_plpgsql_txn(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "plpgsql_txn")
}

func TestReadCommittedLogicCCL_plpgsql_unsupported(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "plpgsql_unsupported")
}

func TestReadCommittedLogicCCL_procedure_params(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "procedure_params")
}

func TestReadCommittedLogicCCL_procedure_plpgsql(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "procedure_plpgsql")
}

func TestReadCommittedLogicCCL_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "read_committed")
}

func TestReadCommittedLogicCCL_redact_descriptor(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "redact_descriptor")
}

func TestReadCommittedLogicCCL_refcursor(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "refcursor")
}

func TestReadCommittedLogicCCL_schema_change_in_txn(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "schema_change_in_txn")
}

func TestReadCommittedLogicCCL_select_for_update_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "select_for_update_read_committed")
}

func TestReadCommittedLogicCCL_show_create(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "show_create")
}

func TestReadCommittedLogicCCL_subject(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "subject")
}

func TestReadCommittedLogicCCL_udf_params(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "udf_params")
}

func TestReadCommittedLogicCCL_udf_plpgsql(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "udf_plpgsql")
}

func TestReadCommittedLogicCCL_udf_rewrite(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "udf_rewrite")
}

func TestReadCommittedLogicCCL_udf_volatility_check(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "udf_volatility_check")
}

func TestReadCommittedLogicCCL_unique_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runCCLLogicTest(t, "unique_read_committed")
}

func TestReadCommittedExecBuild_explain_analyze_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runExecBuildLogicTest(t, "explain_analyze_read_committed")
}

func TestReadCommittedExecBuild_fk_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runExecBuildLogicTest(t, "fk_read_committed")
}

func TestReadCommittedExecBuild_geospatial(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runExecBuildLogicTest(t, "geospatial")
}

func TestReadCommittedExecBuild_select_for_update_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runExecBuildLogicTest(t, "select_for_update_read_committed")
}

func TestReadCommittedExecBuild_unique_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runExecBuildLogicTest(t, "unique_read_committed")
}

func TestReadCommittedExecBuild_update_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runExecBuildLogicTest(t, "update_read_committed")
}

func TestReadCommittedExecBuild_upsert_read_committed(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runExecBuildLogicTest(t, "upsert_read_committed")
}
