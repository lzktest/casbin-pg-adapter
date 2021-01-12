.Phony: test

DATABASE_URL ?= "port=5432 user=postgres host=localhost dbname=postgres sslmode=disable"
test:
	@DATABASE_URL=$(DATABASE_URL) go test -v ./...
