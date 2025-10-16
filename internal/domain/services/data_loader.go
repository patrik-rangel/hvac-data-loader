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
	"github.com/patrik-rangel/hvac-data-loader/internal/resources/s3" // Usaremos tanto S3Reader quanto S3Writer
)

const (
	MaxBatchSize           = 1000 // Quantos documentos inserir no MongoDB por lote
	ErrorChannelBufferSize = 5
)

type DataLoaderService struct {
	repo     gateways.HvacDataRepository
	s3Reader *s3.S3Resource       // Para ler do S3
	s3Writer *s3.S3ResourceWriter // Para escrever no S3
}

func NewDataLoaderService(repo gateways.HvacDataRepository, s3Reader *s3.S3Resource, s3Writer *s3.S3ResourceWriter) *DataLoaderService {
	return &DataLoaderService{
		repo:     repo,
		s3Reader: s3Reader,
		s3Writer: s3Writer,
	}
}

func (s *DataLoaderService) IngestHvacDataFromS3(ctx context.Context, bucketName, objectKey string) error {
	log.Printf("Iniciando ingestão e re-escrita de dados HVAC do s3://%s/%s com particionamento mensal.", bucketName, objectKey)

	bodyStream, err := s.s3Reader.GetObjectStream(ctx, bucketName, objectKey)
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

	// Agora, `recordsByMonth` irá armazenar todos os registros, agrupados por mês,
	// para que possam ser usados tanto para o MongoDB quanto para a escrita no S3.
	recordsByMonth := make(map[string][]entities.HvacSensorData)
	recordsProcessed := 0

	for decoder.More() {
		var hvacRecord entities.HvacSensorData
		err := decoder.Decode(&hvacRecord)
		if err != nil {
			log.Printf("Aviso: Erro ao decodificar registro JSON (registro #%d): %v. Pulando para o próximo.", recordsProcessed+1, err)
			continue
		}

		recordsProcessed++

		monthlyKey := getMonthlyCollectionName(hvacRecord.Timestamp)

		recordsByMonth[monthlyKey] = append(recordsByMonth[monthlyKey], hvacRecord)

		if len(recordsByMonth[monthlyKey]) >= MaxBatchSize {
			wg.Add(1)
			// Crie uma cópia do lote para a goroutine de inserção no MongoDB
			batchToInsert := make([]entities.HvacSensorData, MaxBatchSize)
			copy(batchToInsert, recordsByMonth[monthlyKey][:MaxBatchSize])

			// Remove os itens que foram copiados
			recordsByMonth[monthlyKey] = recordsByMonth[monthlyKey][MaxBatchSize:]

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
			}(ctx, monthlyKey, batchToInsert)
		}
	}

	// Insere quaisquer registros remanescentes no MongoDB
	for monthlyKey, batch := range recordsByMonth {
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
			}(ctx, monthlyKey, batchToInsert)
		}
	}

	wg.Wait()

	select {
	case err = <-errorChannel:
		return fmt.Errorf("um ou mais erros ocorreram durante a ingestão para MongoDB: %w", err)
	default:
		// Nenhum erro fatal reportado
	}

	log.Println("Iniciando re-escrita dos dados particionados no bucket S3 (pasta 'output/')...")

	// Resetar o canal de erros para coletar erros de upload S3
	close(errorChannel)                                     // Fecha o canal atual
	errorChannel = make(chan error, ErrorChannelBufferSize) // Recria um novo canal

	var s3UploadWg sync.WaitGroup

	for monthlyKey, records := range recordsByMonth {
		if len(records) > 0 {
			s3UploadWg.Add(1)

			// Converte o slice de structs para JSON bytes
			jsonBytes, err := json.MarshalIndent(records, "", "  ") // MarshalIndent para JSON formatado
			if err != nil {
				s3UploadWg.Done() // Decrementa o contador antes de continuar
				log.Printf("Erro ao serializar dados para JSON para o mês '%s': %v", monthlyKey, err)
				select {
				case errorChannel <- fmt.Errorf("erro ao serializar JSON para o mês '%s': %w", monthlyKey, err):
				default:
				}
				continue // Pula para o próximo mês
			}

			// Define o nome da chave de saída no S3 (ex: "output/hvac_readings_2024_01.json")
			outputObjectKey := fmt.Sprintf("output/%s.json", monthlyKey)

			go func(ctx context.Context, outputKey string, dataBytes []byte) {
				defer s3UploadWg.Done()
				log.Printf("Iniciando upload do arquivo particionado S3: '%s' para bucket '%s'...", outputKey, bucketName)

				err := s.s3Writer.UploadFile(ctx, bucketName, outputKey, dataBytes, "application/json")
				if err != nil {
					select {
					case errorChannel <- fmt.Errorf("erro ao fazer upload do arquivo particionado S3 '%s': %w", outputKey, err):
					default:
					}
					log.Printf("Erro na goroutine de upload S3 para '%s': %v", outputKey, err)
				} else {
					log.Printf("Arquivo particionado S3 '%s' enviado com sucesso!", outputKey)
				}
			}(ctx, outputObjectKey, jsonBytes)
		}
	}

	s3UploadWg.Wait() // Espera todos os uploads S3 terminarem

	close(errorChannel) // Fecha o canal de erros novamente

	// Verifica se ocorreram erros durante os uploads S3
	select {
	case err = <-errorChannel:
		return fmt.Errorf("um ou mais erros ocorreram durante a re-escrita para S3: %w", err)
	default:
	}

	t, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("falha ao ler token final do JSON: %w", err)
	}
	if t != json.Delim(']') {
		return fmt.Errorf("erro: JSON esperado terminar com array, encontrado %v", t)
	}

	log.Printf("Ingestão e re-escrita do arquivo '%s/%s' concluída. Total de registros processados: %d.", bucketName, objectKey, recordsProcessed)
	return nil
}

// getMonthlyCollectionName gera o nome da coleção/chave no formato 'hvac_readings_YYYY_MM'.
func getMonthlyCollectionName(t time.Time) string {
	return fmt.Sprintf("hvac_readings_%s", t.Format("2006_01")) // "2006_01" formata como "YYYY_MM"
}
