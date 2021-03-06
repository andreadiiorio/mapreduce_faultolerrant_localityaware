package aws_SDK_wrap

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"os"
)

type UPLOADER s3manager.Uploader
type DOWNLOADER s3manager.Downloader

func (*UPLOADER) GobDecode([]byte) error     { return nil }
func (*UPLOADER) GobEncode() ([]byte, error) { return nil, nil }

func (*DOWNLOADER) GobDecode([]byte) error     { return nil }
func (*DOWNLOADER) GobEncode() ([]byte, error) { return nil, nil }

func InitS3Links(region string) (*DOWNLOADER, *UPLOADER) {
	// create 2 s3 session for S3 downloader and uploader
	// Create a single AWS session
	sessionS3, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Fatal(err)
	}
	//// create downloader and uploader on a newly created s3 session
	downloader := s3manager.NewDownloader(sessionS3)
	// Create a single AWS session
	sessionS3, err = session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Fatal(err)
	}
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sessionS3)
	return (*DOWNLOADER)(downloader), (*UPLOADER)(uploader)
}
func GetS3Downloader(region string) *DOWNLOADER {
	sessionS3, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Fatal(err)
	}
	//// create downloader and uploader on a newly created s3 session
	downloader := s3manager.NewDownloader(sessionS3)
	// Create a single AWS session
	return (*DOWNLOADER)(downloader)
}
func GetS3Uploader(region string) *UPLOADER {
	// Create a single AWS session
	sessionS3, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Fatal(err)
	}
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sessionS3)
	return (*UPLOADER)(uploader)
}

/*func main(){
	downloader,uploader:=initS3Links(S3_REGION)
	key:="ASDF"
	data:="FDSA"
	_ = UploadDATA(uploader, data, key,S3_BUCKET)
	buf := make([]byte, len(data))
	_= DownloadDATA(downloader,S3_BUCKET,key,buf)
}*/
//  will  be set file info like content type and encryption on the uploaded file.
func UploadDATA(uploader *UPLOADER, dataStr string, strKey string, bucket string) error {

	// Upload input parameters
	upParams := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(strKey),
		ACL:    aws.String("private"),
		Body:   bytes.NewReader([]byte(dataStr)),
	}

	// Perform upload with options different than the those in the Uploader.
	result, err := (*s3manager.Uploader)(uploader).Upload(upParams)
	if err != nil {
		_, _ = fmt.Fprint(os.Stderr, err, "<-->", result)
		return err
	}

	return nil
}

func DownloadDATA(downloader *DOWNLOADER, bucket, key string, bufOut *[]byte, sizeCheck bool) error {
	//download data from s3 and put it i bufOut, byte buffer pre allocated with expected size, propagated eventual errors

	writer := aws.NewWriteAtBuffer(*bufOut)
	s3InputOption := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	downloadedSize, err := (*s3manager.Downloader)(downloader).Download(writer, s3InputOption)
	if err != nil {
		_, _ = fmt.Fprint(os.Stderr, err)
	}

	if sizeCheck && (int64(len(*bufOut)) != downloadedSize) {
		return errors.New("download size mismatch with expected!\n")
	}
	*bufOut = (*bufOut)[:downloadedSize]
	println("downloaded ", key, " of size ", downloadedSize)
	return nil
}
