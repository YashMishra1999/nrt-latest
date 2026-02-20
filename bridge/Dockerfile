FROM golang:1.24-alpine

WORKDIR /app
COPY main.go .

RUN go mod init bridge && go mod tidy && go build -o bridge

EXPOSE 9000
CMD ["./bridge"]
