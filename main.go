package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

type Device struct {
	Index  int
	UUID   string
	Handle nvml.Device
}

type Metrics struct {
	Temperature uint    `json:"temperature"`
	Power       float32 `json:"power"`
	GpuUsage    uint    `json:"gpu_usage"`
	MemoryTotal float32 `json:"memory_total"`
	MemoryUsed  float32 `json:"memory_used"`
}

func (m Metrics) String() string {
	return fmt.Sprintf("%d,%.2f,%d,%.1f,%.2f", m.Temperature, m.Power, m.GpuUsage, m.MemoryTotal, m.MemoryUsed)
}

func GetDevice(index int) (Device, error) {
	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		log.Fatalf("Unable to get device count: %v", nvml.ErrorString(ret))
	}
	if count <= index-1 {
		log.Fatalf("Device index out of range")
	}
	device, ret := nvml.DeviceGetHandleByIndex(index)
	if ret != nvml.SUCCESS {
		return Device{}, fmt.Errorf("unable to get device at index %d: %v", index, nvml.ErrorString(ret))
	}
	uuid, ret := device.GetUUID()
	if ret != nvml.SUCCESS {
		return Device{}, fmt.Errorf("unable to get uuid of device at index %d: %v", index, nvml.ErrorString(ret))
	}
	return Device{Index: index, UUID: uuid, Handle: device}, nil
}

func (d Device) deviceHandleErrorString(ret nvml.Return) error {
	return fmt.Errorf("%s", nvml.ErrorString(ret))
}

func (d Device) GetTemperature() (uint, error) {
	temp, ret := d.Handle.GetTemperature(nvml.TEMPERATURE_GPU)
	if ret != nvml.SUCCESS {
		return 0, d.deviceHandleErrorString(ret)
	}
	return uint(temp), nil
}

func (d Device) GetPower() (float32, error) {
	power, ret := d.Handle.GetPowerUsage()
	if ret != nvml.SUCCESS {
		return 0, d.deviceHandleErrorString(ret)
	}
	actual := float32(power) / 1000.0
	return actual, nil
}

func (d Device) GetUtilization() (uint, float32, float32, error) {
	memory, ret := d.Handle.GetMemoryInfo()
	if ret != nvml.SUCCESS {
		return 0, 0.0, 0.0, d.deviceHandleErrorString(ret)
	}
	total := float32(memory.Total) / (1 << 30)
	used := float32(memory.Used) / (1 << 30)

	util, ret := d.Handle.GetUtilizationRates()
	if ret != nvml.SUCCESS {
		return 0, 0.0, 0.0, d.deviceHandleErrorString(ret)
	}
	return uint(util.Gpu), total, used, nil
}

func (d Device) GetMetrics() (Metrics, error) {
	temp, err := d.GetTemperature()
	if err != nil {
		return Metrics{}, err
	}
	power, err := d.GetPower()
	if err != nil {
		return Metrics{}, err
	}
	gpu, totalMemory, usedMemory, err := d.GetUtilization()
	if err != nil {
		return Metrics{}, err
	}
	return Metrics{Temperature: temp, Power: power, GpuUsage: gpu, MemoryTotal: totalMemory, MemoryUsed: usedMemory}, nil
}

func (m Metrics) PublishCloudwatchMetrics(ctx context.Context, client *cloudwatch.Client, instanceID string, instanceType string, resolution int32, namespace string) error {
	// Define the dimensions for the metric data
	dimensions := []types.Dimension{
		{
			Name:  aws.String("InstancesId"),
			Value: aws.String(instanceID),
		},
		{
			Name:  aws.String("InstanceType"),
			Value: aws.String(instanceType),
		},
	}

	// Define the metric data to be published
	metricData := []types.MetricDatum{
		{
			MetricName:        aws.String("GPU Usage"),
			Dimensions:        dimensions,
			Unit:              types.StandardUnitPercent,
			StorageResolution: aws.Int32(resolution),
			Value:             aws.Float64(float64(m.GpuUsage)),
		},
		{
			MetricName: aws.String("Memory Used"),
			Dimensions: dimensions,
			// TODO: Double check the units reported by the NVML library
			Unit:              types.StandardUnitMegabytes,
			StorageResolution: aws.Int32(resolution),
			Value:             aws.Float64(float64(m.MemoryUsed)),
		},
		{
			MetricName:        aws.String("Temperature (C)"),
			Dimensions:        dimensions,
			Unit:              types.StandardUnitNone,
			StorageResolution: aws.Int32(resolution),
			Value:             aws.Float64(float64(m.Temperature)),
		},
		{
			MetricName:        aws.String("Power (W)"),
			Dimensions:        dimensions,
			Unit:              types.StandardUnitNone,
			StorageResolution: aws.Int32(resolution),
			Value:             aws.Float64(float64(m.Power)),
		},
	}
	input := &cloudwatch.PutMetricDataInput{
		MetricData: metricData,
		Namespace:  aws.String(namespace),
	}

	// Publish the metrics to CloudWatch
	_, err := client.PutMetricData(ctx, input)
	if err != nil {
		return fmt.Errorf("Unable to publish metrics to CloudWatch: %v", err)
	}

	return nil
}

func main() {
	// We setup a signal handler to catch SIGINT and SIGTERM signals
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Exit(1)
	}()

	ctx := context.Background()
	// We initialize the AWS config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Unable to load AWS config: %v", err)
	}
	cw := cloudwatch.NewFromConfig(cfg)

	ret := nvml.Init()
	if ret != nvml.SUCCESS {
		log.Fatalf("Unable to initialize NVML: %v", nvml.ErrorString(ret))
	}
	defer func() {
		ret := nvml.Shutdown()
		if ret != nvml.SUCCESS {
			log.Fatalf("Unable to shutdown NVML: %v", nvml.ErrorString(ret))
		}
	}()

	device, err := GetDevice(0)
	if err != nil {
		log.Fatalf("Unable to get device: %v", err)
	}

	for {
		metrics, err := device.GetMetrics()
		if err != nil {
			log.Fatalf("Unable to get metrics: %v", err)
		}
		jsonMetrics, err := json.Marshal(metrics)
		if err != nil {
			log.Fatalf("Unable to marshal metrics to JSON: %v", err)
		}
		fmt.Println(string(jsonMetrics))
		// Sleep for 5 seconds
		time.Sleep(time.Second * 5)
	}
}
