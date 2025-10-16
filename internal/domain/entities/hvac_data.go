package entities

import "time"

type HvacSensorData struct {
	Timestamp           time.Time `json:"timestamp"`
	InternalTemperature float64   `json:"internalTemperature"`
	SetPointTemperature float64   `json:"setPointTemperature"`
	SystemStatus        string    `json:"systemStatus"`
	OccupancyStatus     bool      `json:"occupancyStatus"`
	PowerConsumptionKwH float64   `json:"powerConsumptionKwH"`
	OutdoorTemperature  float64   `json:"outdoorTemperature"`
	OutdoorHumidity     float64   `json:"outdoorHumidity"`
	DeviceId            string    `json:"deviceId"`

	SupplyAirTemperature   float64 `json:"supplyAirTemperature"`
	ReturnAirTemperature   float64 `json:"returnAirTemperature"`
	DuctStaticPressurePa   float64 `json:"ductStaticPressurePa"`
	CO2LevelPpm            float64 `json:"co2LevelPpm"`
	RefrigerantPressurePsi float64 `json:"refrigerantPressurePsi"`
	FaultCode              string  `json:"faultCode"`

	AssetModel   string `json:"assetModel"`
	LocationZone string `json:"locationZone"`
}
