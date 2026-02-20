FROM golang:1.24-alpine

WORKDIR /app
COPY main.go .

RUN go mod init app && go mod tidy && go build -o app

CMD ["./app"]
