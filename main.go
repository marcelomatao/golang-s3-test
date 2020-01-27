package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"reflect"

	"github.com/fsnotify/fsnotify"
	"github.com/marcelomatao/golang-s3-test/common"
	"github.com/marcelomatao/golang-s3-test/fetcher"
	"github.com/marcelomatao/golang-s3-test/logger"
	"github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	config fetcher.Config
)

func loadConfFile(cfgPath string) {
	file, err := os.Open(cfgPath)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
			"file":  cfgPath,
		}).Fatal("Could not load config file")
		os.Exit(1)
	}
	defer file.Close()

	file.Sync()
	raw, err := ioutil.ReadAll(file)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
			"file":  cfgPath,
		}).Fatal("Could not find config file")
		os.Exit(1)
	}

	var cfgTmp fetcher.Config
	err = json.Unmarshal(raw, &cfgTmp)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
			"file":  cfgPath,
		}).Fatal("Invalid json file")
		os.Exit(1)
	}

	config = cfgTmp
}

func reloadConfFile(cfgPath string) {

	common.Log.Info("Reloading config...")

	oldConfig := config

	loadConfFile(cfgPath)

	if !reflect.DeepEqual(oldConfig.Logs.Common, config.Logs.Common) {

		l := logger.New(config.Logs.Common)
		common.Log = l.WithFields(logrus.Fields{"tag": "Common"})
	}

	common.Log.Info("Reload config finished!")
}

func uploadFile(service *s3.S3, fileToUpload string, bucket string) error {

	// Upload Files
	upFile, err := os.Open(fileToUpload)
	if err != nil {
		return err
	}
	defer upFile.Close()

	upFileInfo, _ := upFile.Stat()
	var fileSize int64 = upFileInfo.Size()
	fileBuffer := make([]byte, fileSize)
	upFile.Read(fileBuffer)

	_, err = service.PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(fileToUpload),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(fileBuffer),
		ContentLength:        aws.Int64(fileSize),
		ContentType:          aws.String(http.DetectContentType(fileBuffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
	})
	return err
}

func listBuckets(service *s3.S3) error {
	result, err := service.ListBuckets(nil)
	if err != nil {
		return err
	}
	fmt.Println("Buckets:")

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
	return err
}

func listFilesByBucket(service *s3.S3, bucket string) error {
	if &bucket == nil {
		return fmt.Errorf("Bucket name required.")
	}

	resp, err := service.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		return err
	}

	for _, item := range resp.Contents {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
	}
	return err
}

func downloadFileByBucket(session *session.Session, fileAWS string, bucket string, fileToDownload string) error {
	file, err := os.Create(fileToDownload)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", fileToDownload, err)
		return err
	}
	defer file.Close()

	downloader := s3manager.NewDownloader(session)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fileAWS),
		})
	if err != nil {
		return err
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
	return err
}

func copyBetweenBuckets(service *s3.S3, bucketSource string, itemSource string, bucketTarget string, itemTarget string) error {

	source := fmt.Sprintf("%s/%s", bucketSource, itemSource)
	fmt.Printf("Key %s", itemSource)
	_, err := service.CopyObject(&s3.CopyObjectInput{Bucket: aws.String(bucketTarget), CopySource: aws.String(source), Key: aws.String(itemTarget)})
	if err != nil {
		exitErrorf("Unable to copy item from bucket %q to bucket %q, %v", bucketSource, bucketTarget, err)
		return err
	}

	// Wait to see if the item got copied
	err = service.WaitUntilObjectExists(&s3.HeadObjectInput{Bucket: aws.String(bucketTarget), Key: aws.String(itemTarget)})
	if err != nil {
		exitErrorf("Error occurred while waiting for item %q to be copied to bucket %q, %v", bucketSource, itemSource, bucketTarget, err)
		return err
	}

	fmt.Printf("Item %q successfully copied from bucket %q to bucket %q\n", itemSource, bucketSource, bucketTarget)

	return err
}

func deleteAllItemsByBucket(service *s3.S3, bucket string) error {
	iter := s3manager.NewDeleteListIterator(service, &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	})

	err := s3manager.NewBatchDeleteWithClient(service).Delete(aws.BackgroundContext(), iter)
	if err != nil {
		exitErrorf("Unable to delete objects from bucket %q, %v", bucket, err)
		return err
	}

	fmt.Printf("Deleted object(s) from bucket: %s", bucket)
	return err
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func main() {

	var cfgPath string

	flag.StringVar(&cfgPath, "conf", "./config.json", "-conf=/path/config.json")
	flag.Parse()

	loadConfFile(cfgPath)
	l := logger.New(config.Logs.Common)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		l.WithFields(logrus.Fields{
			"error": err,
			"file":  cfgPath,
		}).Fatal("Could not watch for file")
		os.Exit(1)
	}
	defer watcher.Close()
	err = watcher.Add(cfgPath)
	if err != nil {
		l.WithFields(logrus.Fields{
			"error": err,
			"file":  cfgPath,
		}).Fatal("Could not watch for file")
		os.Exit(1)
	}

	common.WatchFileChanges(watcher, cfgPath, reloadConfFile)

	session, err := session.NewSession(&aws.Config{Region: aws.String(config.S3Region)})
	if err != nil {
		log.Fatal(err)
	}

	// Create S3 service client
	service := s3.New(session)

	fileToUpload := "testFile1.txt"

	err = uploadFile(service, fileToUpload, config.S3Bucket)
	if err != nil {
		log.Fatal(err)
	}

	err = listBuckets(service)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	err = listFilesByBucket(service, config.S3Bucket)
	if err != nil {
		exitErrorf("Unable to list files, %v", err)
	}

	fileAWS := fileToUpload
	fileToDownload := fmt.Sprintf("download%d-%s", rand.Int(), fileAWS)
	err = downloadFileByBucket(session, fileAWS, config.S3Bucket, fileToDownload)
	if err != nil {
		exitErrorf("Unable to download a file from a bucket", err)
	}

	sourceItem := fileToUpload
	targetItem := fmt.Sprintf("copy%d-%s", rand.Int(), sourceItem)
	err = copyBetweenBuckets(service, config.S3Bucket, sourceItem, config.S3Bucket, targetItem)
	if err != nil {
		exitErrorf("Unable to copy a file between buckets", err)
	}

	err = deleteAllItemsByBucket(service, config.S3Bucket2)
	if err != nil {
		exitErrorf("Unable to delete all files in a bucket", err)
	}

	err = deleteAllItemsByBucket(service, config.S3Bucket)
	if err != nil {
		exitErrorf("Unable to delete all files in a bucket", err)
	}

}
