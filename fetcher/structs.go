package fetcher

import (
	"github.com/marcelomatao/golang-s3-test/logger"
)

type Config struct {
	Logs struct {
		Common *logger.Config
		Debug  SimpleLog
	}
	S3Retries int
	S3Region  string
	S3Bucket  string
	S3Bucket2 string
}

// SimpleLog struct
type SimpleLog struct {
	File     string
	FileMode string
	FileUser string
}
