package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/obsidiandynamics/goharvest"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"gopkg.in/yaml.v2"

	scribelogrus "github.com/obsidiandynamics/libstdgo/scribe/logrus"
	logrus "github.com/sirupsen/logrus"
)

func panicOnError(scr scribe.Scribe, err error) {
	if err != nil {
		scr.E()("Error: %v", err.Error())
		panic(err)
	}
}

func main() {
	var configFile string
	flag.StringVar(&configFile, "f", "", "Configuration file (shorthand)")
	flag.StringVar(&configFile, "file", "", "Configuration file")
	flag.Parse()

	errorFunc := func(field string) {
		flag.PrintDefaults()
		panic(fmt.Errorf("required '-%s' has not been set", field))
	}
	if configFile == "" {
		errorFunc("f")
	}

	lr := logrus.StandardLogger()
	lr.SetLevel(logrus.TraceLevel)
	scr := scribe.New(scribelogrus.Bind())

	workDir, err := os.Getwd()
	panicOnError(scr, err)
	scr.I()("Starting GoHarvest Reaper")
	executable, err := os.Executable()
	panicOnError(scr, err)
	scr.I()("Executable: %s; working directory: %s", executable, workDir)

	cfgData, err := ioutil.ReadFile(configFile)
	panicOnError(scr, err)
	cfg, err := unmarshal(cfgData)
	panicOnError(scr, err)

	cfg.Harvest.Scribe = scr
	level, err := scribe.ParseLevelName(cfg.Logging.Level)
	panicOnError(scr, err)
	scr.SetEnabled(level.Level)

	h, err := goharvest.New(cfg.Harvest)
	panicOnError(scr, err)

	panicOnError(scr, h.Start())
	panicOnError(scr, h.Await())
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

func (l *LoggingConfig) setDefaults() {
	if l.Level == "" {
		l.Level = scribe.Levels[scribe.Debug].Name
	}
}

type ReaperConfig struct {
	Harvest goharvest.Config `yaml:"harvest"`
	Logging LoggingConfig    `yaml:"logging"`
}

func (r *ReaperConfig) setDefaults() {
	r.Harvest.SetDefaults()
	r.Logging.setDefaults()
}

func unmarshal(in []byte) (ReaperConfig, error) {
	cfg := ReaperConfig{}
	err := yaml.UnmarshalStrict(in, &cfg)
	if err == nil {
		cfg.setDefaults()
	}
	return cfg, err
}
