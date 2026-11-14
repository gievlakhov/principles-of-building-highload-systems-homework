package resilience

//go:generate bash -o pipefail -c "go run go.uber.org/mock/mockgen@v0.6.0 -source=client.go -package=mocks -write_package_comment=false -write_source_comment=false -write_command_comment=false | sed '/^[[:space:]]*\\/\\//d' > mocks/http_client_mock.go"
//go:generate gofmt -w mocks/http_client_mock.go
