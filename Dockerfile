FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/api ./cmd/api

FROM alpine:latest

RUN apk --no-cache add ca-certificates curl && \
    addgroup -S appgroup && \
    adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=builder /app/api /app/api

RUN chown appuser:appgroup /app/api

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

LABEL org.opencontainers.image.title="CleanGo API" \
      org.opencontainers.image.description="Data cleaning and transformation REST API" \
      org.opencontainers.image.source="https://github.com/mstgnz/cleango"

CMD ["/app/api"]
