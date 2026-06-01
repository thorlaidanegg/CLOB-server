module github.com/thorlaidanegg/clob-server

go 1.25.0

require (
	github.com/go-chi/chi/v5 v5.1.0
	github.com/jackc/pgx/v5 v5.6.0
	github.com/redis/go-redis/v9 v9.6.1
	github.com/rs/zerolog v1.33.0
	github.com/thorlaidanegg/clob v0.0.0
	google.golang.org/grpc v1.65.0
)

require (
	github.com/coder/websocket v1.8.14 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/mr-tron/base58 v1.3.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go v1.21.2 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/thorlaidanegg/clob => ../clob
