package s3server

import (
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

//TODO: Move these to settings eventually
const port 8081

func NewS3Server(stopper *stop.Stopper,
	settings *cluster.Settings,
	db *kv.DB,
) {

}
