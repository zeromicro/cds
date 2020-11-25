package iox

import (
	"os"
	"os/exec"
)

func RunAttachedCmd(program string, args ...string) error {
	c := exec.Command(program, args...)
	c.Stdin = os.Stdin
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	e := c.Run()
	return e
}
