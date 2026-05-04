FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod ./
COPY main.go ./
COPY seed_urls.txt ./
COPY common ./common
COPY client ./client
COPY coordinator ./coordinator
COPY worker ./worker

RUN go build -o /out/distributed-search-engine .

FROM alpine:3.22

WORKDIR /app

COPY --from=build /out/distributed-search-engine /app/distributed-search-engine
COPY seed_urls.txt /app/seed_urls.txt

ENTRYPOINT ["/app/distributed-search-engine"]
