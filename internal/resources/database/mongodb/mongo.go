// internal/adapters/database/mongodb/mongo.go
package mongodb

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/patrik-rangel/hvac-data-loader/internal/domain/entities"
	"github.com/patrik-rangel/hvac-data-loader/internal/domain/gateways"
)

type MongoRepository struct {
	client         *mongo.Client
	databaseName   string
	collectionName string
}

func NewMongoRepository(connectionString, databaseName, collectionName string) (gateways.HvacDataRepository, error) {
	clientOptions := options.Client().ApplyURI(connectionString)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("falha ao conectar ao MongoDB: %w", err)
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, fmt.Errorf("falha ao fazer ping no MongoDB: %w", err)
	}

	log.Println("Conectado com sucesso ao MongoDB!")
	return &MongoRepository{
		client:         client,
		databaseName:   databaseName,
		collectionName: collectionName,
	}, nil
}

func (r *MongoRepository) InsertMany(ctx context.Context, data []entities.HvacSensorData) error {
	collection := r.client.Database(r.databaseName).Collection(r.collectionName)

	var documents []interface{}
	for _, item := range data {
		// TODO Criar a conversão de objeto de dominio para o que vai ser inserido
		documents = append(documents, item)
	}

	log.Printf("Inserindo %d documentos na coleção '%s' do banco de dados '%s'...", len(documents), r.collectionName, r.databaseName)

	insertCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res, err := collection.InsertMany(insertCtx, documents)
	if err != nil {
		return fmt.Errorf("falha ao inserir documentos no MongoDB: %w", err)
	}

	log.Printf("Documentos inseridos com sucesso! IDs inseridos: %v", res.InsertedIDs)
	return nil
}

func (r *MongoRepository) CloseConnection(ctx context.Context) error {
	if r.client != nil {
		return r.client.Disconnect(ctx)
	}
	return nil
}
