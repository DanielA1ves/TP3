FROM golang:1.22-bookworm AS build

RUN apt-get update \
  && apt-get install -y --no-install-recommends protobuf-compiler \
  && rm -rf /var/lib/apt/lists/* \
  && go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.1 \
  && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . ./

RUN protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/xmlservice.proto
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/xml-service ./cmd/server

FROM gcr.io/distroless/base-debian12

COPY --from=build /bin/xml-service /xml-service

EXPOSE 8080 9090

ENTRYPOINT ["/xml-service"]
