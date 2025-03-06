FROM golang:1.24-alpine AS builder

WORKDIR /app

# Bağımlılıkları kopyala ve indir
COPY go.mod go.sum* ./
RUN go mod download

# Kaynak kodları kopyala
COPY . .

# API uygulamasını derle
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/api ./cmd/api

# Çalışma zamanı imajı
FROM alpine:latest

WORKDIR /app

# Sertifikaları kopyala
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Derlenmiş uygulamayı kopyala
COPY --from=builder /app/api /app/api

# Uygulama portunu aç
EXPOSE 8080

# Uygulamayı çalıştır
CMD ["/app/api"] 