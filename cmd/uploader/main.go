package main

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	s3Client *s3.S3
	s3Bucket string
	wg       sync.WaitGroup
)

func init() {

	sess, err := session.NewSession(
		&aws.Config{
			Region: aws.String("us-east-2"),
			Credentials: credentials.NewStaticCredentials(
				"Chave",
				"Passe",
				"",
			),
		},
	)
	if err != nil {
		panic(err)
	}
	s3Client = s3.New(sess)
	s3Bucket = "curso-go-diego-bucket"
}

func main() {
	dir, err := os.Open("../../tmp/")
	if err != nil {
		panic(err)
	}
	defer dir.Close()

	uploadControl := make(chan struct{}, 100)
	uploadErrorControl := make(chan string, 10)

	go func() {
		for {
			select {
			case fileName := <-uploadErrorControl:
				fmt.Printf("Arquivo %s falhou o upload\n", fileName)
				uploadControl <- struct{}{}
				wg.Add(1)
				go uploadFile(fileName, uploadControl, uploadErrorControl)
			}
		}
	}()

	for {
		files, err := dir.ReadDir(1)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Erro ao ler o diretorio: %s", err)
			continue
		}
		wg.Add(1)
		uploadControl <- struct{}{}
		go uploadFile(files[0].Name(), uploadControl, uploadErrorControl)
	}
	wg.Wait()
}

func uploadFile(fileName string, uploadControl <-chan struct{}, uploadErrorControl chan<- string) {
	defer wg.Done()
	fmt.Printf("Iniciando upload do arquivo: %s\n", fileName)
	fullFilePath := fmt.Sprintf("../../tmp/%s", fileName)
	f, err := os.Open(fullFilePath)
	if err != nil {
		fmt.Printf("Erro ao abrir o arquivo: %s", fullFilePath)
		<-uploadControl
		uploadErrorControl <- fullFilePath
		return
	}
	defer f.Close()
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(fileName),
		Body:   f,
	})
	if err != nil {
		fmt.Printf("Erro ao fazer o upload do arquivo: %s", fullFilePath)
		<-uploadControl
		uploadErrorControl <- fullFilePath
		return
	}
	<-uploadControl
}
