package domain

// PartitionAssignment is the coordinator's assignment for one partition.
type PartitionAssignment struct {
	Partition   int32
	StartOffset int64
}
