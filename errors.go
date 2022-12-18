package mutcask

import (
	"fmt"
)

var (
	ErrPathUndefined = fmt.Errorf("mutcask: should define path within config")
	ErrPath          = fmt.Errorf("mutcask: path should be directory not file")
	ErrRepoLocked    = fmt.Errorf("mutcask: repo has been locked")
	ErrBufSize       = fmt.Errorf("buf size not match")
)
