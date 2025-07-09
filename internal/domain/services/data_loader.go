package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/patrik-rangel/hvac-data-loader/internal/domain/entities"
	"github.com/patrik-rangel/hvac-data-loader/internal/domain/gateways"
	"github.com/patrik-rangel/hvac-data-loader/internal/resources/s3"
)

const (
	BatchSize = 1000 // Quantos documentos inserir no MongoDB por lote
)

type DataLoaderService struct {
	repo           gateways.HvacDataRepository
	s3             *s3.S3Resource
	databaseName   string
	collectionName string
}

func NewDataLoaderService(repo gateways.HvacDataRepository, s3 *s3.S3Resource, dbName, collName string) *DataLoaderService {
	return &DataLoaderService{
		repo:           repo,
		s3:             s3,
		databaseName:   dbName,
		collectionName: collName,
	}
}

func (s *DataLoaderService) IngestHvacDataFromS3(ctx context.Context, bucketName, objectKey string) error {
	log.Printf("Iniciando ingestão de dados HVAC do s3://%s/%s", bucketName, objectKey)

	bodyStream, err := s.s3.GetObjectStream(ctx, bucketName, objectKey)
	if err != nil {
		return fmt.Errorf("falha ao obter stream do S3: %w", err)
	}
	defer bodyStream.Close() // Garantir que o stream do S3 é fechado

	decoder := json.NewDecoder(bodyStream)

	// Lidar com o colchete de abertura '['
	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("falha ao ler token inicial do JSON: %w", err)
	}
	if t != json.Delim('[') {
		return fmt.Errorf("erro: JSON esperado como array, encontrado %v", t)
	}

	var wg sync.WaitGroup
	errorChannel := make(chan error, 1) // Buffer de 1 para não bloquear a primeira goroutine com erro

	var currentBatch []entities.HvacSensorData
	recordsProcessed := 0
	batchCount := 0

	for decoder.More() {
		var hvacRecord entities.HvacSensorData // Decodifica diretamente para a struct do domínio
		err := decoder.Decode(&hvacRecord)
		if err != nil {
			log.Printf("Aviso: Erro ao decodificar registro JSON: %v. Pulando para o próximo.", err)
			continue
		}

		currentBatch = append(currentBatch, hvacRecord)
		recordsProcessed++

		if len(currentBatch) >= BatchSize {
			batchCount++
			wg.Add(1)
			batchToInsert := make([]entities.HvacSensorData, len(currentBatch)) // Copia o batch
			copy(batchToInsert, currentBatch)

			go func(batchNum int, data []entities.HvacSensorData) {
				defer wg.Done()
				log.Printf("Iniciando inserção do lote %d com %d registros.", batchNum, len(data))
				err := s.repo.InsertMany(ctx, data)
				if err != nil {
					select {
					case errorChannel <- fmt.Errorf("erro ao inserir lote %d: %w", batchNum, err):
					default:
					}
					log.Printf("Erro na goroutine de inserção do lote %d: %v", batchNum, err)
				} else {
					log.Printf("Lote %d inserido com sucesso.", batchNum)
				}
			}(batchCount, batchToInsert)

			currentBatch = nil // Limpa o lote
		}
	}

	// Inserir quaisquer registros remanescentes
	if len(currentBatch) > 0 {
		batchCount++
		wg.Add(1)
		batchToInsert := currentBatch // Não precisa de cópia se é o último
		go func(batchNum int, data []entities.HvacSensorData) {
			defer wg.Done()
			log.Printf("Iniciando inserção do lote final %d com %d registros.", batchNum, len(data))
			err := s.repo.InsertMany(ctx, data) // Chama a interface do repositório!
			if err != nil {
				select {
				case errorChannel <- fmt.Errorf("erro ao inserir lote final %d: %w", batchNum, err):
				default:
				}
				log.Printf("Erro na goroutine de inserção do lote final %d: %v", batchNum, err)
			} else {
				log.Printf("Lote final %d inserido com sucesso.", batchNum)
			}
		}(batchCount, batchToInsert)
	}

	wg.Wait()

	select {
	case err = <-errorChannel:
		return fmt.Errorf("um ou mais erros ocorreram durante a inserção: %w", err)
	default:
		// Nenhum erro no canal
	}

	// Lidar com o colchete de fechamento ']'
	t, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("falha ao ler token final do JSON: %w", err)
	}
	if t != json.Delim(']') {
		return fmt.Errorf("erro: JSON esperado terminar com array, encontrado %v", t)
	}

	log.Printf("Ingestão do arquivo '%s/%s' concluída. Total de registros processados: %d.", bucketName, objectKey, recordsProcessed)
	return nil
}
