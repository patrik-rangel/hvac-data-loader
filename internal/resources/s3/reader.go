package s3

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Resource struct {
	client *s3.Client
}

func NewS3Resource(ctx context.Context) (*S3Resource, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("falha ao carregar a configuração AWS: %w", err)
	}
	return &S3Resource{client: s3.NewFromConfig(cfg)}, nil
}

// GetObjectStream baixa um objeto do S3 e retorna seu conteúdo como um io.ReadCloser.
// É responsabilidade do chamador fechar o io.ReadCloser.
func (a *S3Resource) GetObjectStream(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	log.Printf("Obtendo stream do objeto s3://%s/%s...", bucketName, objectKey)

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}

	resp, err := a.client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("falha ao obter stream do objeto S3 %s/%s: %w", bucketName, objectKey, err)
	}

	log.Printf("Stream do objeto s3://%s/%s obtido com sucesso.", bucketName, objectKey)
	return resp.Body, nil
}
