package vfsdircache

// TreeRefreshToken identifies one active persistent tree refresh.
type TreeRefreshToken struct {
	id uint64
}

// TreeRefreshResult identifies memory paths changed while a tree was read.
type TreeRefreshResult struct {
	StaleDirectories []string
	StaleSubtrees    []string
}
