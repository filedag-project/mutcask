package mutcask

type Config struct {
	Path            string
	CaskNum         uint32
	HintBootReadNum int
	InitBuf         int
}

func defaultConfig() *Config {
	return &Config{
		CaskNum:         256,
		HintBootReadNum: 1000,
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

func InitBufConf(size int) Option {
	return func(cfg *Config) {
		cfg.InitBuf = size
	}
}
