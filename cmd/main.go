package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/joho/godotenv"
	"log"
	"os"
	"strings"

	"github.com/patrik-rangel/hvac-data-loader/internal/domain/services"
	"github.com/patrik-rangel/hvac-data-loader/internal/resources/database/mongodb"
	"github.com/patrik-rangel/hvac-data-loader/internal/resources/s3"
)

func setupDependencies(ctx context.Context) (*services.DataLoaderService, error) {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		return nil, fmt.Errorf("variável de ambiente MONGO_URI não definida")
	}
	mongoDatabase := os.Getenv("MONGO_DATABASE")
	if mongoDatabase == "" {
		return nil, fmt.Errorf("variável de ambiente MONGO_DATABASE não definida")
	}

	mongoRepo, err := mongodb.NewMongoRepository(mongoURI, mongoDatabase)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar repositório MongoDB: %w", err)
	}

	s3Rdr, err := s3.NewS3Resource(ctx)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar adaptador S3: %w", err)
	}

	ingestService := services.NewDataLoaderService(mongoRepo, s3Rdr, mongoDatabase)
	return ingestService, nil
}

func Handler(ctx context.Context, s3Event events.S3Event) {
	log.Println("Handler Lambda invocado. Processando evento S3.")

	ingestService, err := setupDependencies(ctx)
	if err != nil {
		log.Fatalf("Erro fatal ao configurar dependências para Lambda: %v", err)
	}

	for _, record := range s3Event.Records {
		s3Record := record.S3
		bucketName := s3Record.Bucket.Name
		objectKey := s3Record.Object.Key

		log.Printf("Processando arquivo S3: bucket='%s', key='%s'", bucketName, objectKey)

		err := ingestService.IngestHvacDataFromS3(ctx, bucketName, objectKey)
		if err != nil {
			log.Printf("Erro ao processar arquivo S3 '%s/%s': %v", bucketName, objectKey, err)
		} else {
			log.Printf("Arquivo S3 '%s/%s' processado com sucesso.", bucketName, objectKey)
		}
	}
	log.Println("Handler Lambda concluído.")
}

func runNormal(ctx context.Context, bucketName, objectKey string) {
	log.Printf("Executando em modo NORMAL. Processando bucket '%s', key '%s'.", bucketName, objectKey)

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		os.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	if os.Getenv("AWS_REGION") == "" {
		os.Setenv("AWS_REGION", "us-east-1")
	}
	if os.Getenv("AWS_ENDPOINT_URL") == "" {
		os.Setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
	}

	ingestService, err := setupDependencies(ctx)
	if err != nil {
		log.Fatalf("Erro fatal ao configurar dependências para execução normal: %v", err)
	}

	err = ingestService.IngestHvacDataFromS3(ctx, bucketName, objectKey)
	if err != nil {
		log.Printf("DEBUG: err = %#v", err)
		log.Fatalf("Erro fatal ao processar arquivo S3 '%s/%s' em modo normal: %v", bucketName, objectKey, err)
	} else {
		log.Printf("Arquivo S3 '%s/%s' processado com sucesso em modo normal.", bucketName, objectKey)
	}
	log.Println("Execução normal concluída.")
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Aviso: Não foi possível carregar o arquivo .env. Erro:", err)
	}

	// Verifica se está rodando no ambiente Lambda.
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		log.Println("Detectado ambiente AWS Lambda. Iniciando Lambda Handler.")
		lambda.Start(Handler)
	} else {
		log.Println("Detectado ambiente local. Verificando argumentos para execução normal.")
		if len(os.Args) < 3 {
			log.Fatalf("Modo de execução normal requer nome do bucket e chave do objeto como argumentos.\nExemplo: go run main.go hvac-mock-data-output-bucket-seunome hvac_mock_data_A701_20250727_185936.json")
		}
		bucketName := os.Args[1]
		objectKey := os.Args[2]

		if !strings.HasPrefix(objectKey, "hvac_mock_data_") || !strings.HasSuffix(objectKey, ".json") {
			log.Fatalf("A chave do objeto '%s' não parece ser um arquivo JSON de dados HVAC de mock. Verifique o nome do arquivo.", objectKey)
		}

		runNormal(context.Background(), bucketName, objectKey)
	}
}
