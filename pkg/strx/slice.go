package strx

import "strings"

func SlicifyStr(seed string, length int) []string {
	slice := make([]string, length)
	for i := 0; i < length; i++ {
		slice[i] = seed
	}
	return slice
}

func SliceContains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func Slice2to1(slice [][]string) []string {
	out := []string{}
	for _, ss := range slice {
		out = append(out, ss...)
	}
	return out
}

func SliceRepeat(elem string, l int) []string {
	out := []string{}
	for i := 0; i < l; i++ {
		out = append(out, elem)
	}
	return out
}

func DeepSplit(ss []string, sep string) [][]string {
	out := [][]string{}
	for _, s := range ss {
		out = append(out, strings.Split(s, sep))
	}
	return out
}
