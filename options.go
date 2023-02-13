package mutcask

const MAX_PARALLEL_READ = 56

type Config struct {
	Path            string
	CaskNum         uint32
	HintBootReadNum int
	InitBuf         int
	Migrate         bool
	MaxLogFileSize  int
	MaxParallelRead int
}

func defaultConfig() *Config {
	return &Config{
		CaskNum:         256,
		HintBootReadNum: 1000,
		MaxLogFileSize:  1 << 20,
		MaxParallelRead: MAX_PARALLEL_READ,
	}
}

type Option func(cfg *Config)

func CaskNumConf(caskNum int) Option {
	return func(cfg *Config) {
		cfg.CaskNum = uint32(caskNum)
	}
}

func PathConf(dir string) Option {
	return func(cfg *Config) {
		cfg.Path = dir
	}
}

func HintBootReadNumConf(hn int) Option {
	return func(cfg *Config) {
		cfg.HintBootReadNum = hn
	}
}

func MaxParallelReadConf(n int) Option {
	return func(cfg *Config) {
		cfg.MaxParallelRead = n
	}
}

// func InitBufConf(size int) Option {
// 	return func(cfg *Config) {
// 		cfg.InitBuf = size
// 	}
// }

func MigrateConf() Option {
	return func(cfg *Config) {
		cfg.Migrate = true
	}
}
