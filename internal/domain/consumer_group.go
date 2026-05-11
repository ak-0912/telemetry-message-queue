package domain

// PartitionAssignment maps a partition to the offset a consumer should begin
// fetching from after a (re)balance.
type PartitionAssignment struct {
	Partition   int32
	StartOffset int64
}
