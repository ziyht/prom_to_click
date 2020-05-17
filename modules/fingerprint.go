package modules

import "github.com/prometheus/prometheus/storage/remote"

const (
	offset64      uint64 = 14695981039346656037
	prime64       uint64 = 1099511628211
	separatorByte byte   = 255
)

// hashAdd adds a string to a fnv64a hash value, returning the updated hash.
func hashAdd(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// hashAddByte adds a byte to a fnv64a hash value, returning the updated hash.
func hashAddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= prime64
	return h
}

// Fingerprint calculates a fingerprint of SORTED BY NAME labels.
// It is adopted from labelSetToFingerprint, but avoids type conversions and memory allocations.
func Fingerprint(labels []*remote.LabelPair) uint64 {
	if len(labels) == 0 {
		return offset64
	}

	sum := offset64
	for _, l := range labels {
		sum = hashAdd(sum, l.Name)
		sum = hashAddByte(sum, separatorByte)
		sum = hashAdd(sum, l.Value)
		sum = hashAddByte(sum, separatorByte)
	}
	return sum
}