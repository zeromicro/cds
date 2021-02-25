package strx

import "strconv"

// DuplicateName create 'name_1' for 'name', 'name_2' for 'name_1'
func DuplicateName(name string, add int) string {
	if name == "" {
		return "empty_1"
	}
	end := SubAfterLast(name, "_", "")
	if end == "" {
		return name + "_" + strconv.Itoa(add)
	}
	i, e := strconv.Atoi(end)
	if e != nil {
		return name + "_" + strconv.Itoa(add)
	}
	return SubBeforeLast(name, "_", name) + "_" + strconv.Itoa(i+add)
}
