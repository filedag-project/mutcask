package mutcask

type Config struct {
	Path           string
	MaxLogFileSize int64
	Capacity       uint64
}

func defaultConfig() *Config {
	return &Config{
		MaxLogFileSize: 4 << 30,
	}
}

type Option func(cfg *Config)

func PathConf(dir string) Option {
	return func(cfg *Config) {
		cfg.Path = dir
	}
}

func MaxLogFileSize(size int64) Option {
	return func(cfg *Config) {
		cfg.MaxLogFileSize = size
	}
}

func CapacityConf(size uint64) Option {
	return func(cfg *Config) {
		cfg.Capacity = size
	}
}
