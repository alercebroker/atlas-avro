package main

import (
  "encoding/json"
  "fmt"
  "github.com/khezen/avro"
  "io/ioutil"
)

func main() {
  // Read AVRO file as string
  mainSchema, err := ioutil.ReadFile("alert.avsc")

  //mainSchema, err := avro.ParseSchemaFile("alert.avsc")

  // Unmarshal JSON  bytes to Schema interface
/*  var anySchema avro.AnySchema
  err := json.Unmarshal(mainSchema, &anySchema)
  if err != nil {
    panic(err)
  }
  schema := anySchema.Schema()*/
  // Marshal Schema interface to JSON bytes
  mainSchema, err = json.Marshal(mainSchema)
  if err != nil {
    panic(err)
  }
  fmt.Println(string(mainSchema))
}
