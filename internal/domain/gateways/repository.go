package gateways

import (
	"context"

	"github.com/patrik-rangel/hvac-data-loader/internal/domain/entities"
)

type HvacDataRepository interface {
	InsertMany(ctx context.Context, collectionName string, data []entities.HvacSensorData) error
}
