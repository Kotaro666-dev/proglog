module github/Kotaro666-dev/prolog

go 1.13

require (
	github.com/casbin/casbin/v2 v2.55.1
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/raft v1.3.11
	github.com/hashicorp/raft-boltdb v0.0.0-00010101000000-000000000000
	github.com/hashicorp/serf v0.10.1
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.6.0
	github.com/spf13/viper v1.13.0
	github.com/stretchr/testify v1.8.0
	github.com/travisjeffery/go-dynaport v1.0.0
	github.com/tysonmote/gommap v0.0.2
	go.opencensus.io v0.23.0
	go.uber.org/zap v1.23.0
	google.golang.org/genproto v0.0.0-20220519153652-3a47de7e79bd
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.0
)

replace github.com/hashicorp/raft-boltdb => github.com/travisjeffery/raft-boltdb v1.0.0
