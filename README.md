# tish version is fork from Casbin-Postgres-Adapter and gorm-adapter, test from gorm-adapter test file

Casbin Postgres Adapter is the postgres adapter for [Casbin](Casbin)

## Installation
```sh
$ go get github.com/lzktest/casbinpgandadapter
```

## Example
```go
package main

import (
  "database/sql"
  "os"

  "github.com/casbin/casbin/v2"
  casbinpgadapter "github.com/lzktest/casbinpgandadapter"
)

func main() {
  connectionString := "port=30001 user=db password=Aa123456 host=192.168.3.121 dbname=testdb sslmode=disable"
  db, err := sql.Open("postgres", connectionString)
  if err != nil {
    panic(err)
  }

  tableName := "casbin_rule"
  adapter, err := casbinpgadapter.NewAdapterByDBUseTableName(db, "", tableName)
  // other new living example
  // adapter, err := casbinpgadapter.NewAdapterWithDBSchema(db, myDBSchema, tableName)
  // adapter, err := casbinpgadapter.NewAdapterByDB(db)
  // adapter, err := casbinpgadapter.NewAdapter("postgres", connectionString)
  if err != nil {
    panic(err)
  }

  enforcer, err := casbin.NewEnforcer("./examples/model.conf", adapter)
  if err != nil {
    panic(err)
  }

  // Load stored policy from database
  enforcer.LoadPolicy()

  // Do permission checking
  enforcer.Enforce("alice", "data1", "write")

  // Do some mutations
  enforcer.AddPolicy("alice", "data2", "write")
  enforcer.RemovePolicy("alice", "data1", "write")

  // Persist policy to database
  enforcer.SavePolicy()
}
```
