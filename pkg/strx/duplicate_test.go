package strx

import "testing"

func TestDuplicateName(t *testing.T) {
	d := DuplicateName("name", 1)
	if d != `name_1` {
		t.Error("d is not `name_1` , but ", d)
		return
	}
	d = DuplicateName(d, 1)
	if d != `name_2` {
		t.Error("d is not `name_2` , but ", d)
		return
	}
}
