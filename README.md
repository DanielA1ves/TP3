TP3 - Interoperability Microservices (Base Scaffold)

Services:
- crawler (Python, local): crawls NPS state pages, builds a parks CSV, uploads to Supabase S3.
- processor (Node.js, Docker): polls Supabase bucket, calls Nominatim + Open-Meteo (last 30 days + forecast), posts multipart to XML service, receives webhook.
- xml-service (Go, Docker): accepts CSV, converts to XML, stores in Supabase Postgres XML column, triggers webhook, exposes gRPC.
- bi-service (Node.js, Docker): GraphQL API that queries park climate stats via gRPC (XML service handles XPath).

Quick start (Docker):
1) Configure Supabase S3 + Supabase Postgres credentials in `.env` (used by docker-compose).
   Run `sql/schema.sql` in the Supabase SQL editor to create the table.
   Optional: set NOMINATIM_EMAIL and a descriptive NOMINATIM_USER_AGENT.
   Optional: adjust WEATHER_HISTORY_DAYS (default 30) and WEATHER_FORECAST_DAYS (default 7).
2) Run: docker-compose up --build
3) Trigger crawler locally:
   - cd crawler
   - python -m venv .venv
   - .venv\Scripts\activate
   - pip install -r requirements.txt
   - set SUPABASE_S3_ENDPOINT=...
   - set SUPABASE_S3_ACCESS_KEY=...
   - set SUPABASE_S3_SECRET_KEY=...
   - set SUPABASE_S3_BUCKET=...
   - set NPS_STATE_CODES=CA,AZ,UT,CO,NM (optional)
   - set NPS_PARK_LIMIT=80 (optional)
   - set NPS_REQUEST_DELAY_SECONDS=0.5 (optional)
   - set NPS_USER_AGENT=tp3-crawler/1.0 (optional)
   - set CRAWLER_INTERVAL_SECONDS=300 (optional, 5 minutes; 0 = run once)
   - python app.py

GraphQL example (BI service):
- POST http://localhost:4000/graphql
  { "query": "{ parkStats(state: \"CA\", fromDate: \"2025-04-01\", toDate: \"2025-04-30\") { parkCount avgTempMeanC avgPrecipitationValue } }" }
  (fromDate/toDate opcional: default ultimos 30 dias)
  { "query": "{ topParksByTemp(state: \"CA\", limit: 5) { parkCode parkName avgTempMeanC avgPrecipitationValue observationCount } }" }
  { "query": "{ dailyStateSummary(state: \"CA\") { date parkCount avgTempMeanC avgPrecipitationValue } }" }
UI (BI service):
- http://localhost:4000/

SQL examples:
- See sql/example_query.sql

gRPC proto:
- xml-service/proto/xmlservice.proto (Go server)
- bi-service/proto/xmlservice.proto (Node client)

XML-RPC:
- POST http://localhost:8080/rpc (methodName: parkStats)
  Example payload:
  <?xml version="1.0"?>
  <methodCall>
    <methodName>parkStats</methodName>
    <params>
      <param><value><string>CA</string></value></param>
      <param><value><string>2025-04-01</string></value></param>
      <param><value><string>2025-04-30</string></value></param>
    </params>
  </methodCall>

Note:
- The XML service Dockerfile generates gRPC code at build time using protoc.
- If building locally without Docker, run:
  protoc --go_out=. --go-grpc_out=. proto/xmlservice.proto
  from xml-service/.
- The XML service validates incoming XML against `xml-service/schema/parkdata.xsd`
  (override with `XML_SCHEMA_PATH`). Local builds require libxml2 and CGO enabled.
- External APIs used:
  - Nominatim (OpenStreetMap): https://nominatim.openstreetmap.org/
  - Open-Meteo: https://open-meteo.com/
  - Past ranges use precipitation total (mm); future ranges use precipitation probability (%).
