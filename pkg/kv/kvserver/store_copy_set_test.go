package kvserver

import "fmt"

func main() {
	tracker := NewCopySetByLengthTracker()

	arr1 := []int{1, 2, 3}
	arr2 := []int{3, 2, 1}
	arr3 := []int{4, 5, 6}
	arr4 := []int{7, 8, 9, 10, 11}
	arr5 := []int{10, 9, 8, 7}

	fmt.Println("Put arr1:", tracker.Put(arr1)) // Output: false
	fmt.Println("Put arr2:", tracker.Put(arr2)) // Output: true
	fmt.Println("Put arr3:", tracker.Put(arr3)) // Output: false
	fmt.Println("Put arr4:", tracker.Put(arr4)) // Output: false
	fmt.Println("Put arr5:", tracker.Put(arr5)) // Output: true

	inputArr1 := []int{1, 2, 3, 4, 5}
	inputArr2 := []int{7, 8, 9, 10}
	inputArr3 := []int{2, 5}

	longestMatch1 := tracker.LongestMatch(inputArr1)
	fmt.Println("Longest Match 1:", longestMatch1) // Output: 3

	longestMatch2 := tracker.LongestMatch(inputArr2)
	fmt.Println("Longest Match 2:", longestMatch2) // Output: 4

	longestMatch3 := tracker.LongestMatch(inputArr3)
	fmt.Println("Longest Match 3:", longestMatch3) // Output: 0
}
