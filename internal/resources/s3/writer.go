package s3

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3ResourceWriter struct {
	client *s3.Client
}

func NewS3ResourceWriter(ctx context.Context) (*S3ResourceWriter, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("falha ao carregar a configuração AWS para S3 Writer: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &S3ResourceWriter{client: client}, nil
}

// UploadFile faz o upload do conteúdo de um slice de bytes para um bucket S3.
func (a *S3ResourceWriter) UploadFile(ctx context.Context, bucketName, objectKey string, fileContent []byte, contentType string) error {
	log.Printf("Iniciando upload de '%s' para o bucket S3 '%s' (Content-Type: %s)...", objectKey, bucketName, contentType)

	putObjectInput := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(fileContent),
		ContentType: aws.String(contentType),
	}

	_, err := a.client.PutObject(ctx, putObjectInput)
	if err != nil {
		return fmt.Errorf("falha ao fazer upload para S3 para a chave '%s': %w", objectKey, err)
	}

	log.Printf("Upload de '%s' concluído com sucesso!", objectKey)
	return nil
}
