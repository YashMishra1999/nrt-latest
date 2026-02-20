package main

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"

	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const weaviateURL = "http://weaviate:8080"
const className = "FinancialTxn"

// hexID converts OTLP trace/span ID bytes → hex string
func hexID(b []byte) string {
	return hex.EncodeToString(b)
}

// attr extracts string attribute from span
func attr(sp *tracepb.Span, key string) string {
	for _, a := range sp.Attributes {
		if a.Key == key {
			return a.Value.GetStringValue()
		}
	}
	return ""
}

// saveTxn writes object to Weaviate
func saveTxn(obj map[string]any) {
	body, err := json.Marshal(obj)
	if err != nil {
		log.Println("json marshal:", err)
		return
	}

	resp, err := http.Post(weaviateURL+"/v1/objects", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Println("weaviate post:", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

// spanToTxn maps OTLP span → FinancialTxn object
func spanToTxn(sp *tracepb.Span, service string) map[string]any {

	start := int64(sp.StartTimeUnixNano / 1e6) // ms
	end := int64(sp.EndTimeUnixNano / 1e6)
	latency := end - start

	status := "OK"
	if sp.Status != nil && sp.Status.Code == tracepb.Status_STATUS_CODE_ERROR {
		status = "ERROR"
	}

	amountStr := attr(sp, "txn.amount")
	amount, _ := strconv.ParseFloat(amountStr, 64)

	currency := attr(sp, "txn.currency")
	if currency == "" {
		currency = "NA"
	}

	return map[string]any{
		"class": className,
		"properties": map[string]any{
			"service":    service,
			"txn_name":   sp.Name,
			"start_time": start,
			"end_time":   end,
			"latency_ms": latency,
			"status":     status,
			"trace_id":   hexID(sp.TraceId),
			"span_id":    hexID(sp.SpanId),
			"amount":     amount,
			"currency":   currency,
			"error":      status == "ERROR",
		},
	}
}

// ingestHandler receives OTLP HTTP spans
func ingestHandler(w http.ResponseWriter, r *http.Request) {

	var reader io.Reader = r.Body

	// handle gzip compression (OTLP default)
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			log.Println("gzip:", err)
			return
		}
		defer gz.Close()
		reader = gz
	}

	raw, err := io.ReadAll(reader)
	if err != nil {
		log.Println("read body:", err)
		return
	}

	req := &coltrace.ExportTraceServiceRequest{}
	if err := proto.Unmarshal(raw, req); err != nil {
		log.Println("invalid protobuf:", err)
		return
	}

	count := 0

	for _, rs := range req.ResourceSpans {

		// extract service.name
		service := "unknown"
		if rs.Resource != nil {
			for _, a := range rs.Resource.Attributes {
				if a.Key == "service.name" {
					service = a.Value.GetStringValue()
				}
			}
		}

		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				obj := spanToTxn(sp, service)
				saveTxn(obj)
				count++
			}
		}
	}

	log.Printf("financial txns: %d\n", count)
}

func main() {
	http.HandleFunc("/v1/traces", ingestHandler)
	log.Println("bridge listening :9000")
	log.Fatal(http.ListenAndServe(":9000", nil))
}
