package settings

import (
	"fmt"
	"github.com/ipfs/go-log/v2"
	"github.com/jinzhu/configor"
	"time"
)

var Config *TomlConfig

var logging = log.Logger("settings")

func Setup(configFile string) {
	Config = new(TomlConfig)
	err := configor.Load(Config, configFile)
	if err != nil {
		panic(fmt.Sprintf("fail to load app config:\n %v\n", err))
	}
}

func GetDefaultConfig() *TomlConfig {
	return &TomlConfig{
		Api: TomlApi{
			Endpoint: "https://datasets.filedrive.io/",
			Token:    "",
		},
		Car: TomlCar{
			AutoClean:   false,
			CleanPeriod: "24h",
		},
	}
}

type TomlApi struct {
	Endpoint string `toml:"endpoint"`
	Token    string `toml:"token"`
}

type TomlCar struct {
	AutoClean   bool   `toml:"autoClean"`
	CleanPeriod string `toml:"cleanPeriod"`
}

func (t TomlCar) GetCleanPeriod() time.Duration {
	d, err := time.ParseDuration(t.CleanPeriod)
	if err != nil {
		logging.Fatalf("parse cleanPeriod error, %v", err)
	}
	return d
}

type TomlConfig struct {
	Api TomlApi `toml:"api"`
	Car TomlCar `toml:"car"`
}
