// cmd/main.go
package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/patrik-rangel/hvac-data-loader/internal/domain/services"
	"github.com/patrik-rangel/hvac-data-loader/internal/resources/database/mongodb"
	"github.com/patrik-rangel/hvac-data-loader/internal/resources/s3"
)

func Handler(ctx context.Context, s3Event events.S3Event) {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("Erro: Variável de ambiente MONGO_URI não definida.")
	}
	mongoDatabase := os.Getenv("MONGO_DATABASE")
	if mongoDatabase == "" {
		log.Fatal("Erro: Variável de ambiente MONGO_DATABASE não definida.")
	}
	mongoCollection := os.Getenv("MONGO_COLLECTION")
	if mongoCollection == "" {
		log.Fatal("Erro: Variável de ambiente MONGO_COLLECTION não definida.")
	}

	mongoRepo, err := mongodb.NewMongoRepository(mongoURI, mongoDatabase, mongoCollection)
	if err != nil {
		log.Fatalf("Erro fatal ao criar repositório MongoDB: %v", err)
	}

	s3Rdr, err := s3.NewS3Resource(ctx)
	if err != nil {
		log.Fatalf("Erro fatal ao criar adaptador S3: %v", err)
	}

	ingestService := services.NewDataLoaderService(mongoRepo, s3Rdr, mongoDatabase, mongoCollection)

	for _, record := range s3Event.Records {
		s3Record := record.S3
		bucketName := s3Record.Bucket.Name
		objectKey := s3Record.Object.Key

		err := ingestService.IngestHvacDataFromS3(ctx, bucketName, objectKey)
		if err != nil {
			log.Printf("Erro ao processar arquivo S3 '%s/%s': %v", bucketName, objectKey, err)
		}
	}
}

func main() {
	lambda.Start(Handler)
}
