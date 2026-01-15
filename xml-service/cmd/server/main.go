package main

import (
  "context"
  "encoding/csv"
  "encoding/json"
  "encoding/xml"
  "errors"
  "fmt"
  "io"
  "log"
  "net"
  "net/http"
  "os"
  "regexp"
  "strings"
  "time"

  "github.com/jackc/pgx/v5"
  "github.com/jackc/pgx/v5/pgxpool"
  "google.golang.org/grpc"
  "google.golang.org/grpc/codes"
  "google.golang.org/grpc/reflection"
  "google.golang.org/grpc/status"

  xmlservicepb "xml-service/proto"
)

type config struct {
  dbHost             string
  dbPort             string
  dbUser             string
  dbPassword         string
  dbName             string
  httpPort           string
  grpcPort           string
  defaultWebhookURL  string
}

func main() {
  cfg := loadConfig()

  ctx := context.Background()
  pool, err := pgxpool.New(ctx, fmt.Sprintf(
    "postgres://%s:%s@%s:%s/%s",
    cfg.dbUser,
    cfg.dbPassword,
    cfg.dbHost,
    cfg.dbPort,
    cfg.dbName,
  ))
  if err != nil {
    log.Fatalf("db connection failed: %v", err)
  }
  defer pool.Close()

  if err := ensureSchema(ctx, pool); err != nil {
    log.Fatalf("schema init failed: %v", err)
  }

  go func() {
    if err := startHTTP(cfg, pool); err != nil {
      log.Fatalf("http server failed: %v", err)
    }
  }()

  if err := startGRPC(cfg, pool); err != nil {
    log.Fatalf("grpc server failed: %v", err)
  }
}

func loadConfig() config {
  return config{
    dbHost:            getenv("DB_HOST", "localhost"),
    dbPort:            getenv("DB_PORT", "5432"),
    dbUser:            getenv("DB_USER", "tp3"),
    dbPassword:        getenv("DB_PASSWORD", "tp3"),
    dbName:            getenv("DB_NAME", "tp3"),
    httpPort:          getenv("HTTP_PORT", "8080"),
    grpcPort:          getenv("GRPC_PORT", "9090"),
    defaultWebhookURL: getenv("DEFAULT_WEBHOOK_URL", ""),
  }
}

func getenv(key, fallback string) string {
  value := strings.TrimSpace(os.Getenv(key))
  if value == "" {
    return fallback
  }
  return value
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
  _, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS xml_documents (
  id SERIAL PRIMARY KEY,
  xml_documento XML NOT NULL,
  data_criacao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  mapper_version TEXT NOT NULL
);`)
  return err
}

func startHTTP(cfg config, pool *pgxpool.Pool) error {
  mux := http.NewServeMux()
  mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    _ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
  })
  mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
      http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
      return
    }

    if err := r.ParseMultipartForm(20 << 20); err != nil {
      http.Error(w, "invalid multipart payload", http.StatusBadRequest)
      return
    }

    file, _, err := r.FormFile("file")
    if err != nil {
      http.Error(w, "missing file", http.StatusBadRequest)
      return
    }
    defer file.Close()

    mapperVersion := strings.TrimSpace(r.FormValue("mapper_version"))
    if mapperVersion == "" {
      mapperVersion = "v1"
    }

    callbackURL := strings.TrimSpace(r.FormValue("callback_url"))
    if callbackURL == "" {
      callbackURL = cfg.defaultWebhookURL
    }

    xmlDoc, err := csvToXML(file)
    if err != nil {
      http.Error(w, "csv parse failed", http.StatusBadRequest)
      return
    }
    if err := validateXML(xmlDoc); err != nil {
      http.Error(w, "xml validation failed", http.StatusBadRequest)
      return
    }

    var id int64
    err = pool.QueryRow(r.Context(),
      "INSERT INTO xml_documents (xml_documento, mapper_version) VALUES ($1, $2) RETURNING id",
      xmlDoc,
      mapperVersion,
    ).Scan(&id)
    if err != nil {
      http.Error(w, "db insert failed", http.StatusInternalServerError)
      return
    }

    if callbackURL != "" {
      go sendWebhook(callbackURL, id)
    }

    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(map[string]interface{}{
      "id":     id,
      "status": "saved",
    })
  })

  addr := ":" + cfg.httpPort
  log.Printf("HTTP listening on %s", addr)
  return http.ListenAndServe(addr, mux)
}

func startGRPC(cfg config, pool *pgxpool.Pool) error {
  listener, err := net.Listen("tcp", ":"+cfg.grpcPort)
  if err != nil {
    return err
  }

  server := grpc.NewServer()
  xmlservicepb.RegisterXmlServiceServer(server, &grpcServer{db: pool})
  reflection.Register(server)

  log.Printf("gRPC listening on %s", listener.Addr().String())
  return server.Serve(listener)
}

type grpcServer struct {
  xmlservicepb.UnimplementedXmlServiceServer
  db *pgxpool.Pool
}

func (s *grpcServer) GetDocument(ctx context.Context, req *xmlservicepb.GetDocumentRequest) (*xmlservicepb.GetDocumentResponse, error) {
  var (
    id            int64
    xmlDoc        string
    createdAt     time.Time
    mapperVersion string
  )

  err := s.db.QueryRow(ctx,
    "SELECT id, xml_documento::text, data_criacao, mapper_version FROM xml_documents WHERE id = $1",
    req.GetId(),
  ).Scan(&id, &xmlDoc, &createdAt, &mapperVersion)
  if err != nil {
    if errors.Is(err, pgx.ErrNoRows) {
      return nil, status.Error(codes.NotFound, "document not found")
    }
    return nil, status.Error(codes.Internal, "query failed")
  }

  return &xmlservicepb.GetDocumentResponse{
    Id:            id,
    XmlDocument:   xmlDoc,
    CreatedAt:     createdAt.Format(time.RFC3339),
    MapperVersion: mapperVersion,
  }, nil
}

func (s *grpcServer) QueryCurrentPrice(ctx context.Context, req *xmlservicepb.QueryCurrentPriceRequest) (*xmlservicepb.QueryCurrentPriceResponse, error) {
  if strings.TrimSpace(req.GetSku()) == "" {
    return nil, status.Error(codes.InvalidArgument, "sku is required")
  }

  const query = `
SELECT x.preco_atual::float
FROM xml_documents d,
XMLTABLE(
  '//Produto[SKU=$sku]'
  PASSING d.xml_documento, $1::text AS "sku"
  COLUMNS
    sku TEXT PATH 'SKU',
    preco_atual TEXT PATH 'PrecoAtual'
) AS x
WHERE x.sku = $1
ORDER BY d.id DESC
LIMIT 1;`

  var precoAtual float64
  err := s.db.QueryRow(ctx, query, req.GetSku()).Scan(&precoAtual)
  if err != nil {
    if errors.Is(err, pgx.ErrNoRows) {
      return nil, status.Error(codes.NotFound, "sku not found")
    }
    return nil, status.Error(codes.Internal, "query failed")
  }

  return &xmlservicepb.QueryCurrentPriceResponse{
    Sku:        req.GetSku(),
    PrecoAtual: precoAtual,
  }, nil
}

func (s *grpcServer) QueryIncidentStats(ctx context.Context, req *xmlservicepb.QueryIncidentStatsRequest) (*xmlservicepb.QueryIncidentStatsResponse, error) {
  stateInput := strings.TrimSpace(req.GetBorough())
  if stateInput == "" {
    return nil, status.Error(codes.InvalidArgument, "state is required")
  }

  fromDate := strings.TrimSpace(req.GetFromDate())
  toDate := strings.TrimSpace(req.GetToDate())

  if fromDate == "" || toDate == "" {
    now := time.Now().UTC()
    toDate = now.Format("2006-01-02")
    fromDate = now.AddDate(0, 0, -30).Format("2006-01-02")
  }

  if _, err := time.Parse("2006-01-02", fromDate); err != nil {
    return nil, status.Error(codes.InvalidArgument, "from_date must be YYYY-MM-DD")
  }
  if _, err := time.Parse("2006-01-02", toDate); err != nil {
    return nil, status.Error(codes.InvalidArgument, "to_date must be YYYY-MM-DD")
  }

  const query = `
SELECT
  COUNT(DISTINCT NULLIF(x.park_code, ''))::bigint AS park_count,
  COALESCE(AVG(NULLIF(x.temp_mean_c, '')::float), 0) AS avg_temp_mean_c,
  COALESCE(AVG(NULLIF(%s, '')::float), 0) AS avg_precip_value
FROM xml_documents d,
XMLTABLE(
  '//Produto'
  PASSING d.xml_documento
  COLUMNS
    observation_date TEXT PATH 'ObservationDate',
    state_code TEXT PATH 'StateCode',
    state_name TEXT PATH 'StateName',
    park_code TEXT PATH 'ParkCode',
    temp_mean_c TEXT PATH 'TempMeanC',
    precipitation_sum_mm TEXT PATH 'PrecipitationSumMm',
    precipitation_probability TEXT PATH 'PrecipitationProbability'
) AS x
WHERE (x.state_code = $1 OR x.state_name ILIKE $2)
  AND NULLIF(x.observation_date, '')::date BETWEEN $3::date AND $4::date;`

  var (
    parkCount int64
    avgTempMean float64
    avgPrecipValue float64
  )
  stateName, stateCode := normalizeStateInput(stateInput)
  stateNamePattern := stateName
  if stateNamePattern == "" {
    stateNamePattern = stateInput
  }

  today := time.Now().UTC().Format("2006-01-02")
  useForecast := toDate > today
  precipColumn := "x.precipitation_sum_mm"
  if useForecast {
    precipColumn = "x.precipitation_probability"
  }
  finalQuery := fmt.Sprintf(query, precipColumn)

  err := s.db.QueryRow(ctx, finalQuery, stateCode, stateNamePattern, fromDate, toDate).
    Scan(&parkCount, &avgTempMean, &avgPrecipValue)
  if err != nil {
    return nil, status.Error(codes.Internal, "query failed")
  }

  return &xmlservicepb.QueryIncidentStatsResponse{
    Borough:             stateInput,
    IncidentCount:       parkCount,
    AvgInspections_30D:  avgTempMean,
    AvgNotApprovedRate:  avgPrecipValue,
  }, nil
}

var tagSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

func csvToXML(reader io.Reader) (string, error) {
  csvReader := csv.NewReader(reader)
  csvReader.FieldsPerRecord = -1

  headers, err := csvReader.Read()
  if err != nil {
    return "", err
  }

  var builder strings.Builder
  builder.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
  builder.WriteString("<Mercado><Produtos>")

  for {
    record, err := csvReader.Read()
    if errors.Is(err, io.EOF) {
      break
    }
    if err != nil {
      return "", err
    }

    builder.WriteString("<Produto>")
    for idx, header := range headers {
      tag := normalizeTag(header)
      builder.WriteString("<")
      builder.WriteString(tag)
      builder.WriteString(">")
      if idx < len(record) {
        _ = xml.EscapeText(&builder, []byte(record[idx]))
      }
      builder.WriteString("</")
      builder.WriteString(tag)
      builder.WriteString(">")
    }
    builder.WriteString("</Produto>")
  }

  builder.WriteString("</Produtos></Mercado>")
  return builder.String(), nil
}

func normalizeTag(value string) string {
  trimmed := strings.TrimSpace(value)
  if trimmed == "" {
    return "Campo"
  }
  sanitized := tagSanitizer.ReplaceAllString(trimmed, "_")
  if sanitized == "" {
    return "Campo"
  }
  if sanitized[0] >= '0' && sanitized[0] <= '9' {
    return "F_" + sanitized
  }
  return sanitized
}

func normalizeStateInput(value string) (string, string) {
  trimmed := strings.TrimSpace(value)
  if trimmed == "" {
    return "", ""
  }
  upper := strings.ToUpper(trimmed)
  if len(upper) <= 2 {
    return "", upper
  }
  return trimmed, upper
}

func validateXML(xmlDoc string) error {
  decoder := xml.NewDecoder(strings.NewReader(xmlDoc))
  for {
    if _, err := decoder.Token(); err != nil {
      if errors.Is(err, io.EOF) {
        return nil
      }
      return err
    }
  }
}

func sendWebhook(url string, id int64) {
  payload := map[string]interface{}{
    "id":     id,
    "status": "saved",
  }
  body, _ := json.Marshal(payload)

  client := &http.Client{Timeout: 8 * time.Second}
  resp, err := client.Post(url, "application/json", strings.NewReader(string(body)))
  if err != nil {
    log.Printf("webhook failed: %v", err)
    return
  }
  _ = resp.Body.Close()
}
