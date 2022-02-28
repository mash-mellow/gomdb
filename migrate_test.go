package mdb

import (
	"testing"
)

func TestForceSync(t *testing.T)  {
	err := ForceSync("utf8", &School{}, &Class{}, &Student{})
	if err != nil {
		t.Fatal(err)
	}
}


