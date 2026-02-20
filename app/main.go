package main

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func initTracer() (*sdktrace.TracerProvider, error) {
	ctx := context.Background()

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("otel-collector:4317"),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	bsp := sdktrace.NewBatchSpanProcessor(
		exp,
		sdktrace.WithBatchTimeout(1*time.Second),
		sdktrace.WithMaxExportBatchSize(1),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(bsp),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

func main() {
	tp, err := initTracer()
	if err != nil {
		log.Fatal(err)
	}
	defer tp.Shutdown(context.Background())

	tracer := otel.Tracer("victoria-demo")

	for {
		_, span := tracer.Start(context.Background(), "checkout-flow")
		time.Sleep(200 * time.Millisecond)
		span.End()
		time.Sleep(1 * time.Second)
	}
}
