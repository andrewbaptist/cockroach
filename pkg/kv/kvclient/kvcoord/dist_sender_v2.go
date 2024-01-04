package kvcoord

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

type transport struct {
}

// Send synchronously sends the BatchRequest rpc to the next replica.
func (t transport) Send(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
	return nil, nil
}

type distSender struct {
	retryOptions retry.Options
}

type routingInfo struct {
}

func (i routingInfo) update(resp *kvpb.BatchResponse) bool {
	return false
}

type sortedReplicas struct {
}

func (r sortedReplicas) Next() *transport {
	return &transport{}
}

func (i routingInfo) sortedReplicas() sortedReplicas {
	return sortedReplicas{}
}

func (i routingInfo) triedAllReplicas() *kvpb.BatchResponse {
	return nil
}

func (i routingInfo) terminal(resp *kvpb.BatchResponse) bool {

}

type batchIterator struct {
}

func (i batchIterator) Next() *routedRequest {
	return nil
}

func (i batchIterator) mergedResponse() *kvpb.BatchResponse {
	return nil
}

type routedRequest struct {
	concurrent bool
	req        *kvpb.BatchRequest
	routing    routingInfo
}

func (r routedRequest) setResp(batchResponse *kvpb.BatchResponse) {

}

func createBatchIterator(req *kvpb.BatchRequest) batchIterator {
	return batchIterator{}
}
func isSuccess(*kvpb.BatchResponse) bool {
	return true
}

func isSendError(error) bool {
	return false
}

func retriableFailure(*kvpb.BatchResponse) bool {
	return false
}

func terminal(*kvpb.BatchResponse) bool {
	return false
}

func (ds *distSender) Send(ctx context.Context, req *kvpb.BatchRequest) *kvpb.BatchResponse {
	iter := createBatchIterator(req)
	for next := iter.Next(); next != nil; next = iter.Next() {
		if next.concurrent {
			go func() {
				next.setResp(ds.sendPartialBatch(ctx, next.req, next.routing))
			}()
		} else {
			next.setResp(ds.sendPartialBatch(ctx, next.req, next.routing))
		}
	}
	return iter.mergedResponse()
}

func (ds *distSender) sendPartialBatch(ctx context.Context, req *kvpb.BatchRequest, routing routingInfo) *kvpb.BatchResponse {
	for r := retry.StartWithCtx(ctx, ds.retryOptions); r.Next(); {
		resp := ds.sendToReplicas(ctx, req, routing)
		// success, routing changed, ambiguous, non-retriable?
		if routing.terminal(resp) {
			return resp
		}
		// TODO: Is there any non-terminal error?
	}
	return routing.triedAllReplicas()
}

func (ds *distSender) sendToReplicas(ctx context.Context, ba *kvpb.BatchRequest, routing routingInfo) *kvpb.BatchResponse {
	iter := routing.sortedReplicas()
	for next := iter.Next(); next != nil; next = iter.Next() {
		br, err := next.Send(ctx, ba)
		if isSendError(err) {
			continue
		}
		if terminal(br) {
			return br
		}
	}
	return routing.triedAllReplicas()
}
