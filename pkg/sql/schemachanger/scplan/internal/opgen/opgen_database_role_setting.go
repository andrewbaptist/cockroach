// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.DatabaseRoleSetting)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.DatabaseRoleSetting) *scop.NotImplemented {
					return notImplemented(this)
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.DatabaseRoleSetting) *scop.RemoveDatabaseRoleSettings {
					return &scop.RemoveDatabaseRoleSettings{
						DatabaseID: this.DatabaseID,
					}
				}),
			),
		),
	)
}
