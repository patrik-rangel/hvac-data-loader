package entities

import "time"

type HvacSensorData struct {
	Timestamp           time.Time
	InternalTemperature float64
	SetPointTemperature float64
	SystemStatus        string
	OccupancyStatus     bool
	PowerConsumptionKwH float64
	OutdoorTemperature  float64
	OutdoorHumidity     float64
	DeviceId            string
}
