package kvserver

import (
	"fmt"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TODO: Add a async add method - using a channel to signal when the array is
// TODO: Add a method to lookup best match length.
// Always stored stored by StoreID.
type storeIds struct {
	voters []roachpb.StoreID
}

func FromStoreDescriptor(descriptor *roachpb.RangeDescriptor) storeIds {
	replicas := descriptor.Replicas().Voters().Descriptors()
	voters := make([]roachpb.StoreID, len(replicas))
	for i, descriptor := range replicas {
		voters[i] = descriptor.StoreID
	}
	slices.Sort(voters)
	return storeIds{voters: voters}
}

// CopySetTracker tracks arrays of integers of a given length.
type CopySetTracker struct {
	trackers map[int]*copySetByLength // Map of length to tracker
}

// copySetByLength trackes unique sets of integers of a given length.
type copySetByLength struct {
	data map[string]storeIds
}

func trackerByLength() *copySetByLength {
	return &copySetByLength{
		data: make(map[string]storeIds),
	}
}

func NewCopySetTracker() *CopySetTracker {
	return &CopySetTracker{
		trackers: make(map[int]*copySetByLength),
	}
}

func (t *CopySetTracker) Put(arr []roachpb.StoreID) bool {
	length := len(arr)
	if _, ok := t.trackers[length]; !ok {
		t.trackers[length] = trackerByLength()
	}

	sortedArr := make([]roachpb.StoreID, length)
	copy(sortedArr, arr)
	slices.Sort(sortedArr)
	key := fmt.Sprintf("%v", sortedArr)

	tracker := t.trackers[length]
	if _, ok := tracker.data[key]; ok {
		return true // Array already exists
	}

	tracker.data[key] = true

	return false // Array added successfully
}

func (t *CopySetTracker) LongestMatch(arr []int) int {
	length := len(arr)
	if _, ok := t.trackers[length]; !ok {
		return 0 // No arrays of this length stored
	}

	tracker := t.trackers[length]
	longestMatch := 0

	for _, val := range arr {
		if tracker.elementMap[val] > 0 {
			longestMatch++
			tracker.elementMap[val]--
		}
	}

	return longestMatch
}
