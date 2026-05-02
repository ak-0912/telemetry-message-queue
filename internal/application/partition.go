package application

import "hash/fnv"

// PartitionFor routes a key to a partition index in [0, count).
func PartitionFor(key string, count int) int {
	if count <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(count))
}
