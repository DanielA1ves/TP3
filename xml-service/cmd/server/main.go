package main

import (
  "bytes"
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
  "net/url"
  "os"
  "regexp"
  "strings"
  "time"

  "github.com/jackc/pgx/v5"
  "github.com/jackc/pgx/v5/pgxpool"
  "github.com/lestrrat-go/libxml2"
  "github.com/lestrrat-go/libxml2/xsd"
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
  dbSSLMode          string
  dbURL              string
  httpPort           string
  grpcPort           string
  defaultWebhookURL  string
  xmlSchemaPath      string
}

var (
  errStateRequired    = errors.New("state is required")
  errInvalidFromDate  = errors.New("from_date must be YYYY-MM-DD")
  errInvalidToDate    = errors.New("to_date must be YYYY-MM-DD")
  xmlSchema           *xsd.Schema
)

func main() {
  cfg := loadConfig()

  ctx := context.Background()
  schema, err := loadXMLSchema(cfg.xmlSchemaPath)
  if err != nil {
    log.Fatalf("xsd load failed: %v", err)
  }
  if schema != nil {
    xmlSchema = schema
    defer xmlSchema.Free()
    log.Printf("XSD validation enabled (%s)", cfg.xmlSchemaPath)
  }
  dbURL := buildDBURL(cfg)
  poolConfig, err := pgxpool.ParseConfig(dbURL)
  if err != nil {
    log.Fatalf("db config failed: %v", err)
  }
  poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
  poolConfig.ConnConfig.StatementCacheCapacity = 0
  poolConfig.ConnConfig.DescriptionCacheCapacity = 0
  pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
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
    dbSSLMode:         getenv("DB_SSLMODE", "disable"),
    dbURL:             strings.TrimSpace(os.Getenv("DB_URL")),
    httpPort:          getenv("HTTP_PORT", "8080"),
    grpcPort:          getenv("GRPC_PORT", "9090"),
    defaultWebhookURL: getenv("DEFAULT_WEBHOOK_URL", ""),
    xmlSchemaPath:     getenv("XML_SCHEMA_PATH", "schema/parkdata.xsd"),
  }
}

func getenv(key, fallback string) string {
  value := strings.TrimSpace(os.Getenv(key))
  if value == "" {
    return fallback
  }
  return value
}

func buildDBURL(cfg config) string {
  if strings.TrimSpace(cfg.dbURL) != "" {
    return cfg.dbURL
  }

  dbHost := strings.TrimSpace(cfg.dbHost)
  dbPort := strings.TrimSpace(cfg.dbPort)
  dbName := strings.TrimSpace(cfg.dbName)
  sslMode := strings.TrimSpace(cfg.dbSSLMode)

  if sslMode == "" {
    sslMode = "disable"
  }

  dbURL := &url.URL{
    Scheme: "postgres",
    User:   url.UserPassword(cfg.dbUser, cfg.dbPassword),
    Host:   net.JoinHostPort(dbHost, dbPort),
    Path:   dbName,
  }
  query := dbURL.Query()
  query.Set("sslmode", sslMode)
  dbURL.RawQuery = query.Encode()

  return dbURL.String()
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
      log.Printf("upload parse error: %v", err)
      http.Error(w, "invalid multipart payload", http.StatusBadRequest)
      return
    }

    file, _, err := r.FormFile("file")
    if err != nil {
      log.Printf("upload missing file: %v", err)
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
      log.Printf("csv parse failed: %v", err)
      http.Error(w, "csv parse failed", http.StatusBadRequest)
      return
    }
    if err := validateXML(xmlDoc); err != nil {
      log.Printf("xml validation failed: %v", err)
      http.Error(w, "xml validation failed", http.StatusBadRequest)
      return
    }
    if xmlSchema != nil {
      log.Printf("xml validation ok (xsd)")
    } else {
      log.Printf("xml validation ok (well-formed)")
    }

    var id int64
    err = pool.QueryRow(r.Context(),
      "INSERT INTO xml_documents (xml_documento, mapper_version) VALUES ($1, $2) RETURNING id",
      xmlDoc,
      mapperVersion,
    ).Scan(&id)
    if err != nil {
      log.Printf("db insert failed: %v", err)
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
  mux.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
      http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
      return
    }

    body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
    if err != nil {
      http.Error(w, "invalid request body", http.StatusBadRequest)
      return
    }

    var call xmlrpcMethodCall
    if err := xml.Unmarshal(body, &call); err != nil {
      writeXMLRPCFault(w, http.StatusBadRequest, 400, "invalid xml-rpc payload")
      return
    }

    switch strings.TrimSpace(call.MethodName) {
    case "parkStats":
      state := call.ParamAsString(0)
      fromDate := call.ParamAsString(1)
      toDate := call.ParamAsString(2)
      if state == "" {
        writeXMLRPCFault(w, http.StatusBadRequest, 400, "state is required")
        return
      }

      parkCount, avgTemp, avgPrecip, err := queryParkStats(r.Context(), pool, state, fromDate, toDate)
      if err != nil {
        writeXMLRPCFault(w, http.StatusInternalServerError, 500, err.Error())
        return
      }

      response := xmlrpcMethodResponse{
        Params: []xmlrpcParam{{
          Value: xmlrpcValue{
            Struct: &xmlrpcStruct{
              Members: []xmlrpcMember{
                xmlrpcMember{Name: "state", Value: xmlrpcValue{String: &state}},
                xmlrpcMember{Name: "parkCount", Value: xmlrpcValue{Int: intPtr(int(parkCount))}},
                xmlrpcMember{Name: "avgTempMeanC", Value: xmlrpcValue{Double: &avgTemp}},
                xmlrpcMember{Name: "avgPrecipitationValue", Value: xmlrpcValue{Double: &avgPrecip}},
              },
            },
          },
        }},
      }

      writeXMLRPCResponse(w, http.StatusOK, response)
    default:
      writeXMLRPCFault(w, http.StatusBadRequest, 400, "unknown method")
    }
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

func (s *grpcServer) QueryParkStats(ctx context.Context, req *xmlservicepb.QueryParkStatsRequest) (*xmlservicepb.QueryParkStatsResponse, error) {
  stateInput := strings.TrimSpace(req.GetState())
  if stateInput == "" {
    return nil, status.Error(codes.InvalidArgument, "state is required")
  }

  fromDate := strings.TrimSpace(req.GetFromDate())
  toDate := strings.TrimSpace(req.GetToDate())
  parkCount, avgTempMean, avgPrecipValue, err := queryParkStats(ctx, s.db, stateInput, fromDate, toDate)
  if err != nil {
    if isInvalidArgument(err) {
      return nil, status.Error(codes.InvalidArgument, err.Error())
    }
    return nil, status.Error(codes.Internal, "query failed")
  }

  return &xmlservicepb.QueryParkStatsResponse{
    State:                stateInput,
    ParkCount:            parkCount,
    AvgTempMeanC:         avgTempMean,
    AvgPrecipitationValue: avgPrecipValue,
  }, nil
}

func (s *grpcServer) QueryTopParksByTemp(ctx context.Context, req *xmlservicepb.QueryTopParksByTempRequest) (*xmlservicepb.QueryTopParksByTempResponse, error) {
  stateInput := strings.TrimSpace(req.GetState())
  if stateInput == "" {
    return nil, status.Error(codes.InvalidArgument, "state is required")
  }

  fromDate := strings.TrimSpace(req.GetFromDate())
  toDate := strings.TrimSpace(req.GetToDate())
  limit := req.GetLimit()

  rows, err := queryTopParksByTemp(ctx, s.db, stateInput, fromDate, toDate, limit)
  if err != nil {
    if isInvalidArgument(err) {
      return nil, status.Error(codes.InvalidArgument, err.Error())
    }
    return nil, status.Error(codes.Internal, "query failed")
  }

  items := make([]*xmlservicepb.ParkTemperatureRank, 0, len(rows))
  for _, row := range rows {
    items = append(items, &xmlservicepb.ParkTemperatureRank{
      ParkCode:             row.ParkCode,
      ParkName:             row.ParkName,
      AvgTempMeanC:         row.AvgTempMeanC,
      AvgPrecipitationValue: row.AvgPrecipValue,
      ObservationCount:     int32(row.ObservationCount),
    })
  }

  return &xmlservicepb.QueryTopParksByTempResponse{Items: items}, nil
}

func (s *grpcServer) QueryDailyStateSummary(ctx context.Context, req *xmlservicepb.QueryDailyStateSummaryRequest) (*xmlservicepb.QueryDailyStateSummaryResponse, error) {
  stateInput := strings.TrimSpace(req.GetState())
  if stateInput == "" {
    return nil, status.Error(codes.InvalidArgument, "state is required")
  }

  fromDate := strings.TrimSpace(req.GetFromDate())
  toDate := strings.TrimSpace(req.GetToDate())

  rows, err := queryDailyStateSummary(ctx, s.db, stateInput, fromDate, toDate)
  if err != nil {
    if isInvalidArgument(err) {
      return nil, status.Error(codes.InvalidArgument, err.Error())
    }
    return nil, status.Error(codes.Internal, "query failed")
  }

  items := make([]*xmlservicepb.DailyStateSummary, 0, len(rows))
  for _, row := range rows {
    items = append(items, &xmlservicepb.DailyStateSummary{
      Date:                 row.Date,
      ParkCount:            int32(row.ParkCount),
      AvgTempMeanC:         row.AvgTempMeanC,
      AvgPrecipitationValue: row.AvgPrecipValue,
    })
  }

  return &xmlservicepb.QueryDailyStateSummaryResponse{Items: items}, nil
}

var headerCleaner = regexp.MustCompile(`[^a-z0-9]+`)

type ParkData struct {
  XMLName     xml.Name `xml:"ParkData"`
  GeneratedAt string   `xml:"generatedAt,attr,omitempty"`
  Parks       []Park   `xml:"Parks>Park"`
}

type Park struct {
  Code     string   `xml:"Code"`
  Name     string   `xml:"Name"`
  State    State    `xml:"State"`
  Location Location `xml:"Location"`
  Geo      Geo      `xml:"Geo"`
  Weather  Weather  `xml:"Weather"`
}

type State struct {
  Code string `xml:"Code,omitempty"`
  Name string `xml:"Name,omitempty"`
}

type Location struct {
  Text        string `xml:"Text,omitempty"`
  Designation string `xml:"Designation,omitempty"`
  Description string `xml:"Description,omitempty"`
  Url         string `xml:"Url,omitempty"`
  CrawlDate   string `xml:"CrawlDate,omitempty"`
}

type Geo struct {
  Latitude     string `xml:"Latitude,omitempty"`
  Longitude    string `xml:"Longitude,omitempty"`
  County       string `xml:"County,omitempty"`
  Municipality string `xml:"Municipality,omitempty"`
  DisplayName  string `xml:"DisplayName,omitempty"`
  Source       string `xml:"Source,omitempty"`
}

type Weather struct {
  Observations []Observation `xml:"Observation"`
}

type Observation struct {
  Date          string         `xml:"Date,omitempty"`
  TempMeanC     string         `xml:"TempMeanC,omitempty"`
  Precipitation *Precipitation `xml:"Precipitation,omitempty"`
  Timezone      string         `xml:"Timezone,omitempty"`
  Source        string         `xml:"Source,omitempty"`
}

type Precipitation struct {
  SumMm       string `xml:"SumMm,omitempty"`
  Probability string `xml:"Probability,omitempty"`
}

type xmlrpcMethodCall struct {
  XMLName    xml.Name      `xml:"methodCall"`
  MethodName string        `xml:"methodName"`
  Params     []xmlrpcParam `xml:"params>param"`
}

type xmlrpcParam struct {
  Value xmlrpcValue `xml:"value"`
}

type xmlrpcValue struct {
  String  *string      `xml:"string,omitempty"`
  Int     *int         `xml:"int,omitempty"`
  I4      *int         `xml:"i4,omitempty"`
  Double  *float64     `xml:"double,omitempty"`
  Boolean *int         `xml:"boolean,omitempty"`
  Struct  *xmlrpcStruct `xml:"struct,omitempty"`
}

type xmlrpcStruct struct {
  Members []xmlrpcMember `xml:"member"`
}

type xmlrpcMember struct {
  Name  string      `xml:"name"`
  Value xmlrpcValue `xml:"value"`
}

type xmlrpcFault struct {
  Value xmlrpcValue `xml:"value"`
}

type xmlrpcMethodResponse struct {
  XMLName xml.Name      `xml:"methodResponse"`
  Params  []xmlrpcParam `xml:"params>param,omitempty"`
  Fault   *xmlrpcFault  `xml:"fault,omitempty"`
}

func (call xmlrpcMethodCall) ParamAsString(idx int) string {
  if idx < 0 || idx >= len(call.Params) {
    return ""
  }
  return call.Params[idx].Value.AsString()
}

func (value xmlrpcValue) AsString() string {
  switch {
  case value.String != nil:
    return strings.TrimSpace(*value.String)
  case value.Int != nil:
    return fmt.Sprintf("%d", *value.Int)
  case value.I4 != nil:
    return fmt.Sprintf("%d", *value.I4)
  case value.Double != nil:
    return fmt.Sprintf("%v", *value.Double)
  case value.Boolean != nil:
    if *value.Boolean == 0 {
      return "false"
    }
    return "true"
  default:
    return ""
  }
}

func csvToXML(reader io.Reader) (string, error) {
  csvReader := csv.NewReader(reader)
  csvReader.FieldsPerRecord = -1

  headers, err := csvReader.Read()
  if err != nil {
    return "", err
  }

  headerIndex := buildHeaderIndex(headers)
  parks := make(map[string]*Park)
  order := make([]string, 0)

  for {
    record, err := csvReader.Read()
    if errors.Is(err, io.EOF) {
      break
    }
    if err != nil {
      return "", err
    }

    parkCode := normalizeText(getField(record, headerIndex, "ParkCode"))
    parkName := normalizeText(getField(record, headerIndex, "ParkName"))
    stateCode := normalizeText(getField(record, headerIndex, "StateCode"))
    stateName := normalizeText(getField(record, headerIndex, "StateName"))
    location := normalizeText(getField(record, headerIndex, "Location"))
    designation := normalizeText(getField(record, headerIndex, "Designation"))
    description := normalizeText(getField(record, headerIndex, "Description"))
    parkUrl := normalizeText(getField(record, headerIndex, "ParkUrl"))
    crawlDate := normalizeText(getField(record, headerIndex, "CrawlDate"))

    key := parkCode
    if key == "" {
      key = strings.ToUpper(strings.TrimSpace(parkName + "|" + stateCode))
    }
    if key == "" {
      continue
    }

    park, exists := parks[key]
    if !exists {
      park = &Park{
        Code: parkCode,
        Name: parkName,
        State: State{
          Code: stateCode,
          Name: stateName,
        },
        Location: Location{
          Text:        location,
          Designation: designation,
          Description: description,
          Url:         parkUrl,
          CrawlDate:   crawlDate,
        },
        Geo: Geo{
          Latitude:     normalizeText(getField(record, headerIndex, "Latitude")),
          Longitude:    normalizeText(getField(record, headerIndex, "Longitude")),
          County:       normalizeText(getField(record, headerIndex, "County")),
          Municipality: normalizeText(getField(record, headerIndex, "Municipality")),
          DisplayName:  normalizeText(getField(record, headerIndex, "GeoDisplayName")),
          Source:       normalizeText(getField(record, headerIndex, "GeocodeSource")),
        },
        Weather: Weather{},
      }
      parks[key] = park
      order = append(order, key)
    } else {
      if park.Code == "" {
        park.Code = parkCode
      }
      if park.Name == "" {
        park.Name = parkName
      }
      if park.State.Code == "" {
        park.State.Code = stateCode
      }
      if park.State.Name == "" {
        park.State.Name = stateName
      }
      if park.Location.Text == "" {
        park.Location.Text = location
      }
      if park.Location.Designation == "" {
        park.Location.Designation = designation
      }
      if park.Location.Description == "" {
        park.Location.Description = description
      }
      if park.Location.Url == "" {
        park.Location.Url = parkUrl
      }
      if park.Location.CrawlDate == "" {
        park.Location.CrawlDate = crawlDate
      }
      if park.Geo.Latitude == "" {
        park.Geo.Latitude = normalizeText(getField(record, headerIndex, "Latitude"))
      }
      if park.Geo.Longitude == "" {
        park.Geo.Longitude = normalizeText(getField(record, headerIndex, "Longitude"))
      }
      if park.Geo.County == "" {
        park.Geo.County = normalizeText(getField(record, headerIndex, "County"))
      }
      if park.Geo.Municipality == "" {
        park.Geo.Municipality = normalizeText(getField(record, headerIndex, "Municipality"))
      }
      if park.Geo.DisplayName == "" {
        park.Geo.DisplayName = normalizeText(getField(record, headerIndex, "GeoDisplayName"))
      }
      if park.Geo.Source == "" {
        park.Geo.Source = normalizeText(getField(record, headerIndex, "GeocodeSource"))
      }
    }

    observationDate := normalizeText(getField(record, headerIndex, "ObservationDate"))
    tempMeanC := normalizeText(getField(record, headerIndex, "TempMeanC"))
    precipSumMm := normalizeText(getField(record, headerIndex, "PrecipitationSumMm"))
    precipProbability := normalizeText(getField(record, headerIndex, "PrecipitationProbability"))
    timezone := normalizeText(getField(record, headerIndex, "Timezone"))
    weatherSource := normalizeText(getField(record, headerIndex, "WeatherSource"))

    if observationDate != "" || tempMeanC != "" || precipSumMm != "" || precipProbability != "" {
      var precip *Precipitation
      if precipSumMm != "" || precipProbability != "" {
        precip = &Precipitation{
          SumMm:       precipSumMm,
          Probability: precipProbability,
        }
      }
      park.Weather.Observations = append(park.Weather.Observations, Observation{
        Date:          observationDate,
        TempMeanC:     tempMeanC,
        Precipitation: precip,
        Timezone:      timezone,
        Source:        weatherSource,
      })
    }
  }

  parksList := make([]Park, 0, len(order))
  for _, key := range order {
    if park := parks[key]; park != nil {
      parksList = append(parksList, *park)
    }
  }

  parkData := ParkData{
    GeneratedAt: time.Now().UTC().Format(time.RFC3339),
    Parks:       parksList,
  }

  var buf bytes.Buffer
  buf.WriteString(xml.Header)
  encoder := xml.NewEncoder(&buf)
  encoder.Indent("", "  ")
  if err := encoder.Encode(parkData); err != nil {
    return "", err
  }
  if err := encoder.Flush(); err != nil {
    return "", err
  }

  return buf.String(), nil
}

func buildHeaderIndex(headers []string) map[string]int {
  index := make(map[string]int, len(headers))
  for idx, header := range headers {
    key := normalizeHeaderKey(header)
    if key == "" {
      continue
    }
    if _, exists := index[key]; !exists {
      index[key] = idx
    }
  }
  return index
}

func normalizeHeaderKey(value string) string {
  normalized := strings.ToLower(strings.TrimSpace(value))
  if normalized == "" {
    return ""
  }
  return headerCleaner.ReplaceAllString(normalized, "")
}

func getField(record []string, index map[string]int, header string) string {
  key := normalizeHeaderKey(header)
  if key == "" {
    return ""
  }
  idx, ok := index[key]
  if !ok || idx < 0 || idx >= len(record) {
    return ""
  }
  return record[idx]
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

func normalizeText(value string) string {
  return strings.TrimSpace(value)
}

func resolveDateRange(fromDate, toDate string) (string, string, error) {
  fromDate = strings.TrimSpace(fromDate)
  toDate = strings.TrimSpace(toDate)
  if fromDate == "" || toDate == "" {
    now := time.Now().UTC()
    toDate = now.Format("2006-01-02")
    fromDate = now.AddDate(0, 0, -30).Format("2006-01-02")
  }

  if _, err := time.Parse("2006-01-02", fromDate); err != nil {
    return "", "", errInvalidFromDate
  }
  if _, err := time.Parse("2006-01-02", toDate); err != nil {
    return "", "", errInvalidToDate
  }

  return fromDate, toDate, nil
}

func resolveStateParams(stateInput string) (string, string) {
  stateInput = strings.TrimSpace(stateInput)
  stateName, stateCode := normalizeStateInput(stateInput)
  statePattern := stateName
  if statePattern == "" {
    statePattern = stateInput
  }
  return stateCode, statePattern
}

func resolvePrecipColumn(toDate string) string {
  today := time.Now().UTC().Format("2006-01-02")
  if toDate > today {
    return "x.precipitation_probability"
  }
  return "x.precipitation_sum_mm"
}

func safeFloatExpr(column string) string {
  return fmt.Sprintf(
    "CASE WHEN %s ~ '^[-]?[0-9]+(\\.[0-9]+)?$' THEN %s::float ELSE NULL END",
    column,
    column,
  )
}

func sanitizeLimit(value int32) int32 {
  if value <= 0 {
    return 5
  }
  if value > 50 {
    return 50
  }
  return value
}

func isInvalidArgument(err error) bool {
  if err == nil {
    return false
  }
  return errors.Is(err, errStateRequired) ||
    errors.Is(err, errInvalidFromDate) ||
    errors.Is(err, errInvalidToDate)
}

func validateXML(xmlDoc string) error {
  if xmlSchema != nil {
    doc, err := libxml2.ParseString(xmlDoc)
    if err != nil {
      return err
    }
    defer doc.Free()

    if err := xmlSchema.Validate(doc); err != nil {
      return formatSchemaError(err)
    }
    return nil
  }

  return validateXMLWellFormed(xmlDoc)
}

func validateXMLWellFormed(xmlDoc string) error {
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

func loadXMLSchema(path string) (*xsd.Schema, error) {
  trimmed := strings.TrimSpace(path)
  if trimmed == "" {
    return nil, nil
  }
  if _, err := os.Stat(trimmed); err != nil {
    return nil, err
  }
  schema, err := xsd.ParseFromFile(trimmed)
  if err != nil {
    return nil, err
  }
  return schema, nil
}

func formatSchemaError(err error) error {
  schemaErr, ok := err.(xsd.SchemaValidationError)
  if !ok {
    return err
  }
  details := schemaErr.Errors()
  if len(details) == 0 {
    return err
  }
  messages := make([]string, 0, len(details))
  for _, detail := range details {
    messages = append(messages, detail.Error())
  }
  return fmt.Errorf("xml schema validation failed: %s", strings.Join(messages, "; "))
}

type parkTempRow struct {
  ParkCode         string
  ParkName         string
  AvgTempMeanC     float64
  AvgPrecipValue   float64
  ObservationCount int64
}

type dailySummaryRow struct {
  Date           string
  ParkCount      int64
  AvgTempMeanC   float64
  AvgPrecipValue float64
}

func queryParkStats(ctx context.Context, db *pgxpool.Pool, stateInput, fromDate, toDate string) (int64, float64, float64, error) {
  if strings.TrimSpace(stateInput) == "" {
    return 0, 0, 0, errStateRequired
  }

  fromDate, toDate, err := resolveDateRange(fromDate, toDate)
  if err != nil {
    return 0, 0, 0, err
  }

  const query = `
SELECT
  COUNT(DISTINCT NULLIF(x.park_code, ''))::bigint AS park_count,
  COALESCE(AVG(%s), 0) AS avg_temp_mean_c,
  COALESCE(AVG(%s), 0) AS avg_precip_value
FROM xml_documents d,
XMLTABLE(
  '/ParkData/Parks/Park/Weather/Observation'
  PASSING d.xml_documento
  COLUMNS
    observation_date TEXT PATH 'Date',
    state_code TEXT PATH '../../State/Code',
    state_name TEXT PATH '../../State/Name',
    park_code TEXT PATH '../../Code',
    temp_mean_c TEXT PATH 'TempMeanC',
    precipitation_sum_mm TEXT PATH 'Precipitation/SumMm',
    precipitation_probability TEXT PATH 'Precipitation/Probability'
) AS x
WHERE (x.state_code = $1 OR x.state_name ILIKE $2)
  AND NULLIF(x.observation_date, '')::date BETWEEN $3::date AND $4::date;`

  stateCode, statePattern := resolveStateParams(stateInput)
  precipColumn := resolvePrecipColumn(toDate)
  tempExpr := safeFloatExpr("x.temp_mean_c")
  precipExpr := safeFloatExpr(precipColumn)
  finalQuery := fmt.Sprintf(query, tempExpr, precipExpr)
  var parkCount int64
  var avgTempMean float64
  var avgPrecipValue float64

  if err := db.QueryRow(ctx, finalQuery, stateCode, statePattern, fromDate, toDate).
    Scan(&parkCount, &avgTempMean, &avgPrecipValue); err != nil {
    return 0, 0, 0, err
  }

  return parkCount, avgTempMean, avgPrecipValue, nil
}

func queryTopParksByTemp(ctx context.Context, db *pgxpool.Pool, stateInput, fromDate, toDate string, limit int32) ([]parkTempRow, error) {
  if strings.TrimSpace(stateInput) == "" {
    return nil, errStateRequired
  }

  fromDate, toDate, err := resolveDateRange(fromDate, toDate)
  if err != nil {
    return nil, err
  }

  const query = `
SELECT
  NULLIF(x.park_code, '') AS park_code,
  MAX(NULLIF(x.park_name, '')) AS park_name,
  COALESCE(AVG(%s), 0) AS avg_temp_mean_c,
  COALESCE(AVG(%s), 0) AS avg_precip_value,
  COUNT(*)::int AS observation_count
FROM xml_documents d,
XMLTABLE(
  '/ParkData/Parks/Park/Weather/Observation'
  PASSING d.xml_documento
  COLUMNS
    observation_date TEXT PATH 'Date',
    state_code TEXT PATH '../../State/Code',
    state_name TEXT PATH '../../State/Name',
    park_code TEXT PATH '../../Code',
    park_name TEXT PATH '../../Name',
    temp_mean_c TEXT PATH 'TempMeanC',
    precipitation_sum_mm TEXT PATH 'Precipitation/SumMm',
    precipitation_probability TEXT PATH 'Precipitation/Probability'
) AS x
WHERE (x.state_code = $1 OR x.state_name ILIKE $2)
  AND NULLIF(x.observation_date, '')::date BETWEEN $3::date AND $4::date
GROUP BY NULLIF(x.park_code, '')
HAVING NULLIF(x.park_code, '') IS NOT NULL
ORDER BY avg_temp_mean_c DESC
LIMIT $5;`

  stateCode, statePattern := resolveStateParams(stateInput)
  precipColumn := resolvePrecipColumn(toDate)
  safeLimit := sanitizeLimit(limit)
  tempExpr := safeFloatExpr("x.temp_mean_c")
  precipExpr := safeFloatExpr(precipColumn)
  finalQuery := fmt.Sprintf(query, tempExpr, precipExpr)

  rows, err := db.Query(ctx, finalQuery, stateCode, statePattern, fromDate, toDate, safeLimit)
  if err != nil {
    return nil, err
  }
  defer rows.Close()

  results := make([]parkTempRow, 0)
  for rows.Next() {
    var row parkTempRow
    var observationCount int32
    if err := rows.Scan(&row.ParkCode, &row.ParkName, &row.AvgTempMeanC, &row.AvgPrecipValue, &observationCount); err != nil {
      return nil, err
    }
    row.ObservationCount = int64(observationCount)
    results = append(results, row)
  }
  if rows.Err() != nil {
    return nil, rows.Err()
  }

  return results, nil
}

func queryDailyStateSummary(ctx context.Context, db *pgxpool.Pool, stateInput, fromDate, toDate string) ([]dailySummaryRow, error) {
  if strings.TrimSpace(stateInput) == "" {
    return nil, errStateRequired
  }

  fromDate, toDate, err := resolveDateRange(fromDate, toDate)
  if err != nil {
    return nil, err
  }

  const query = `
SELECT
  NULLIF(x.observation_date, '')::date AS observation_date,
  COUNT(DISTINCT NULLIF(x.park_code, ''))::int AS park_count,
  COALESCE(AVG(%s), 0) AS avg_temp_mean_c,
  COALESCE(AVG(%s), 0) AS avg_precip_value
FROM xml_documents d,
XMLTABLE(
  '/ParkData/Parks/Park/Weather/Observation'
  PASSING d.xml_documento
  COLUMNS
    observation_date TEXT PATH 'Date',
    state_code TEXT PATH '../../State/Code',
    state_name TEXT PATH '../../State/Name',
    park_code TEXT PATH '../../Code',
    temp_mean_c TEXT PATH 'TempMeanC',
    precipitation_sum_mm TEXT PATH 'Precipitation/SumMm',
    precipitation_probability TEXT PATH 'Precipitation/Probability'
) AS x
WHERE (x.state_code = $1 OR x.state_name ILIKE $2)
  AND NULLIF(x.observation_date, '')::date BETWEEN $3::date AND $4::date
GROUP BY observation_date
ORDER BY observation_date;`

  stateCode, statePattern := resolveStateParams(stateInput)
  precipColumn := resolvePrecipColumn(toDate)
  tempExpr := safeFloatExpr("x.temp_mean_c")
  precipExpr := safeFloatExpr(precipColumn)
  finalQuery := fmt.Sprintf(query, tempExpr, precipExpr)

  rows, err := db.Query(ctx, finalQuery, stateCode, statePattern, fromDate, toDate)
  if err != nil {
    return nil, err
  }
  defer rows.Close()

  results := make([]dailySummaryRow, 0)
  for rows.Next() {
    var row dailySummaryRow
    var observationDate time.Time
    var parkCount int32
    if err := rows.Scan(&observationDate, &parkCount, &row.AvgTempMeanC, &row.AvgPrecipValue); err != nil {
      return nil, err
    }
    row.Date = observationDate.Format("2006-01-02")
    row.ParkCount = int64(parkCount)
    results = append(results, row)
  }
  if rows.Err() != nil {
    return nil, rows.Err()
  }

  return results, nil
}

func writeXMLRPCResponse(w http.ResponseWriter, status int, response xmlrpcMethodResponse) {
  w.Header().Set("Content-Type", "text/xml")
  w.WriteHeader(status)
  var buf bytes.Buffer
  buf.WriteString(xml.Header)
  encoder := xml.NewEncoder(&buf)
  encoder.Indent("", "  ")
  if err := encoder.Encode(response); err != nil {
    http.Error(w, "xml-rpc encoding failed", http.StatusInternalServerError)
    return
  }
  _ = encoder.Flush()
  _, _ = w.Write(buf.Bytes())
}

func writeXMLRPCFault(w http.ResponseWriter, status int, code int, message string) {
  msg := message
  response := xmlrpcMethodResponse{
    Fault: &xmlrpcFault{
      Value: xmlrpcValue{
        Struct: &xmlrpcStruct{
          Members: []xmlrpcMember{
            {Name: "faultCode", Value: xmlrpcValue{Int: intPtr(code)}},
            {Name: "faultString", Value: xmlrpcValue{String: &msg}},
          },
        },
      },
    },
  }
  writeXMLRPCResponse(w, status, response)
}

func intPtr(value int) *int {
  return &value
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
