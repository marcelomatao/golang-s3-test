package common

import (
	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
)

var (
	// Log - Entry for writing common logs
	Log *logrus.Entry
)

type loadFile func(string)

// WatchFileChanges monitors if a file changes and calls the function passed as parameter
func WatchFileChanges(watcher *fsnotify.Watcher, fileName string, fn loadFile) {
	writeCh := make(chan bool)
	errCh := make(chan error)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				switch {
				case event.Op&fsnotify.Write == fsnotify.Write:
					writeCh <- true
				}
			case err := <-watcher.Errors:
				errCh <- err
			}
		}
	}()

	go func() {
		for {
			select {
			case <-writeCh:
				fn(fileName)
			}
		}
	}()
}
