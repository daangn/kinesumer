package collection

// ContainsS checks if a slice contains a given string.
func ContainsS(s []string, v string) bool {
	for _, e := range s {
		if e == v {
			return true
		}
	}
	return false
}

// EqualsSS checks if given string slices are equal.
func EqualsSS(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
