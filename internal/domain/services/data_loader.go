package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/patrik-rangel/hvac-data-loader/internal/domain/entities"
	"github.com/patrik-rangel/hvac-data-loader/internal/domain/gateways"
	"github.com/patrik-rangel/hvac-data-loader/internal/resources/s3"
)

const (
	MaxBatchSize           = 1000 // Quantos documentos inserir no MongoDB por lote
	ErrorChannelBufferSize = 5
)

type DataLoaderService struct {
	repo gateways.HvacDataRepository
	s3   *s3.S3Resource
}

func NewDataLoaderService(repo gateways.HvacDataRepository, s3 *s3.S3Resource, dbName string) *DataLoaderService {
	return &DataLoaderService{
		repo: repo,
		s3:   s3,
	}
}

func (s *DataLoaderService) IngestHvacDataFromS3(ctx context.Context, bucketName, objectKey string) error {
	log.Printf("Iniciando ingestão de dados HVAC do s3://%s/%s com particionamento mensal.", bucketName, objectKey)

	bodyStream, err := s.s3.GetObjectStream(ctx, bucketName, objectKey)
	if err != nil {
		return fmt.Errorf("falha ao obter stream do S3: %w", err)
	}
	defer bodyStream.Close()

	decoder := json.NewDecoder(bodyStream)

	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("falha ao ler token inicial do JSON: %w", err)
	}
	if t != json.Delim('[') {
		return fmt.Errorf("erro: JSON esperado como array, encontrado %v", t)
	}

	var wg sync.WaitGroup
	errorChannel := make(chan error, ErrorChannelBufferSize)

	batchesByCollection := make(map[string][]entities.HvacSensorData)
	recordsProcessed := 0

	for decoder.More() {
		var hvacRecord entities.HvacSensorData
		err := decoder.Decode(&hvacRecord)
		if err != nil {
			log.Printf("Aviso: Erro ao decodificar registro JSON (registro #%d): %v. Pulando para o próximo.", recordsProcessed+1, err)
			continue
		}

		recordsProcessed++

		collectionName := getMonthlyCollectionName(hvacRecord.Timestamp)

		batchesByCollection[collectionName] = append(batchesByCollection[collectionName], hvacRecord)

		// Verifica se algum lote atingiu o tamanho máximo
		for colName, batch := range batchesByCollection {
			if len(batch) >= MaxBatchSize {
				wg.Add(1)
				batchToInsert := make([]entities.HvacSensorData, len(batch))
				copy(batchToInsert, batch)

				go func(ctx context.Context, collectionName string, data []entities.HvacSensorData) {
					defer wg.Done()
					log.Printf("Iniciando inserção do lote de %d registros na coleção '%s'.", len(data), collectionName)
					err := s.repo.InsertMany(ctx, collectionName, data)
					if err != nil {
						select {
						case errorChannel <- fmt.Errorf("erro ao inserir lote na coleção '%s': %w", collectionName, err):
						default:
						}
						log.Printf("Erro na goroutine de inserção para coleção '%s': %v", collectionName, err)
					} else {
						log.Printf("Lote de %d registros inserido com sucesso na coleção '%s'.", len(data), collectionName)
					}
				}(ctx, colName, batchToInsert)

				// Limpar o lote que foi despachado
				delete(batchesByCollection, colName)
			}
		}
	}

	for colName, batch := range batchesByCollection {
		if len(batch) > 0 {
			wg.Add(1)
			batchToInsert := batch
			go func(ctx context.Context, collectionName string, data []entities.HvacSensorData) {
				defer wg.Done()
				log.Printf("Iniciando inserção do lote final de %d registros na coleção '%s'.", len(data), collectionName)
				err := s.repo.InsertMany(ctx, collectionName, data)
				if err != nil {
					select {
					case errorChannel <- fmt.Errorf("erro ao inserir lote final na coleção '%s': %w", collectionName, err):
					default:
					}
					log.Printf("Erro na goroutine de inserção do lote final para coleção '%s': %v", collectionName, err)
				} else {
					log.Printf("Lote final de %d registros inserido com sucesso na coleção '%s'.", len(data), collectionName)
				}
			}(ctx, colName, batchToInsert)
		}
	}

	wg.Wait()

	close(errorChannel)

	select {
	case err = <-errorChannel:
		return fmt.Errorf("um ou mais erros ocorreram durante a ingestão: %w", err)
	default:
	}

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

func getMonthlyCollectionName(t time.Time) string {
	return fmt.Sprintf("hvac_readings_%s", t.Format("2006_01")) // "2006_01" formata como "YYYY_MM"
}
