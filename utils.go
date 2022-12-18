package mutcask

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

func LoadSys(repoPath string) (*SysState, error) {
	b, err := ioutil.ReadFile(filepath.Join(repoPath, SYS_STATE_PATH))
	if err != nil {
		return nil, err
	}
	ss := &SysState{}
	if err := json.Unmarshal(b, ss); err != nil {
		return nil, err
	}
	return ss, nil
}

func InitSysJson(repoPath string, ss *SysState) error {
	p := filepath.Join(repoPath, SYS_STATE_PATH)
	if _, err := os.Stat(p); err == nil {
		return fmt.Errorf("sys state json already exist")
	}
	b, err := json.Marshal(ss)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(p, b, 0644)
}

func FlushSys(repoPath string, ss *SysState) error {
	p := filepath.Join(repoPath, SYS_STATE_PATH)
	b, err := json.Marshal(ss)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(p, b, 0644)
}

func VLogPath(repoPath string, id uint64) string {
	return filepath.Join(repoPath, V_LOG_DIR, fmt.Sprintf("%08d%s", id, V_LOG_SUFFIX))
}

func TimestampPrefix(pre string) string {
	num := rand.Intn(1000)
	t := time.Now().Unix()
	return fmt.Sprintf("%s_%d_%d", pre, t, num)
}
