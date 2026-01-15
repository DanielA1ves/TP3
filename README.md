TP3 - Interoperability Microservices (Base Scaffold)

Services:
- crawler (Python, local): crawls NPS state pages, builds a parks CSV, uploads to Supabase S3.
- processor (Node.js, Docker): polls Supabase bucket, calls Nominatim + Open-Meteo (last 30 days + forecast), posts multipart to XML service, receives webhook.
- xml-service (Go, Docker): accepts CSV, converts to XML, stores in PostgreSQL XML column, triggers webhook, exposes gRPC.
- bi-service (Node.js, Docker): GraphQL API that queries park climate stats via gRPC.

Quick start (Docker):
1) Configure Supabase S3 credentials in `.env` (used by docker-compose).
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
   - python app.py

GraphQL example (BI service):
- POST http://localhost:4000/graphql
  { "query": "{ parkStats(state: \"CA\", fromDate: \"2025-04-01\", toDate: \"2025-04-30\") { parkCount avgTempMeanC avgPrecipitationValue } }" }
  (fromDate/toDate opcional: default ultimos 30 dias)
UI (BI service):
- http://localhost:4000/

SQL examples:
- See sql/example_query.sql

gRPC proto:
- xml-service/proto/xmlservice.proto (Go server)
- bi-service/proto/xmlservice.proto (Node client)

Note:
- The XML service Dockerfile generates gRPC code at build time using protoc.
- If building locally without Docker, run:
  protoc --go_out=. --go-grpc_out=. proto/xmlservice.proto
  from xml-service/.
- External APIs used:
  - Nominatim (OpenStreetMap): https://nominatim.openstreetmap.org/
  - Open-Meteo: https://open-meteo.com/
  - Past ranges use precipitation total (mm); future ranges use precipitation probability (%).
