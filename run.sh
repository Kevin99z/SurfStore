go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l &
go run cmd/SurfstoreServerExec/main.go -s block -p 8082 -l &
go run cmd/SurfstoreServerExec/main.go -s meta -l localhost:8081 localhost:8082 && fg