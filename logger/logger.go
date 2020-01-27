package logger

import (
	"log"
	"os"
	"strings"

	"github.com/marcelomatao/golang-s3-test/common"
	"github.com/sirupsen/logrus"
)

// Config config
type Config struct {
	Level          string
	File           string
	TimeFormat     string
	RotationPeriod string
	Symlink        bool
	JSON           bool
	Stdout         bool
}

// New new
func New(c *Config) *logrus.Logger {

	if c == nil {
		log.Fatal("log configuration is empty")
	}

	l := logrus.New()

	logLevel := map[string]logrus.Level{
		"debug": logrus.DebugLevel,
		"info":  logrus.InfoLevel,
		"warn":  logrus.WarnLevel,
		"error": logrus.ErrorLevel,
		"fatal": logrus.FatalLevel,
		"panic": logrus.FatalLevel,
	}

	level, ok := logLevel[strings.ToLower(c.Level)]
	if !ok {
		l.Fatalf("could not find log level %s", c.Level)
	}
	l.SetLevel(level)

	if c.JSON {
		l.Formatter = &logrus.JSONFormatter{}
	}

	if c.Stdout {
		l.Out = os.Stdout
		return l
	}

	var logRotator = new(common.Rotator)
	logRotator.Add(&common.LogFile{
		NamePrefix: c.File,
		TimeFormat: c.TimeFormat,
		Symlink:    c.Symlink,
		CallBack: func(f *os.File) {
			l.Out = f
		},
	})

	logRotator.Start(c.RotationPeriod)

	return l

}
