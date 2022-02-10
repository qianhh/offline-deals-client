package settings

import (
	"fmt"
	"github.com/ipfs/go-log/v2"
	"github.com/jinzhu/configor"
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
	}
}

type TomlApi struct {
	Endpoint string `toml:"endpoint"`
	Token    string `toml:"token"`
}

type TomlConfig struct {
	Api TomlApi `toml:"api"`
}
