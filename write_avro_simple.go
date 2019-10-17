package main

import (
  "bytes"
  "fmt"
  "gopkg.in/avro.v0"
  "log"
  "os"
)

type SimpleRecord struct {
        A int64  `avro:"a"`
        B string `avro:"b"`
}

func main() {
  // Parse the schema file
  schema, err := avro.ParseSchemaFile("alert_simple.avsc")
  if err != nil {
    log.Fatal(err)
  }
  // Open file to write to
  f, err := os.Create("file.avro")
  if err != nil {
    fmt.Println(err)
    return
  }
  // Create buffer to store data
  var buf bytes.Buffer
  encoder := avro.NewBinaryEncoder(&buf)
  // Create DatumWriter and set schema
  datumWriter := avro.NewSpecificDatumWriter()
  datumWriter.SetSchema(schema)
  // Create a simple record
  simple := &SimpleRecord{A: 27, B: "foo",}
  // Write the data to the buffer through datumWriter
  err = datumWriter.Write(simple, encoder)
  if err != nil {
    log.Fatal(err)
  }
  // Create a fileWriter
  fileWriter, err := avro.NewDataFileWriter(f, schema, datumWriter)
  if err != nil {
    log.Fatal(err)
  }
  // fileWriter needs an argument
  err = fileWriter.Write(simple)
  err = fileWriter.Flush()
  if err != nil {
    fmt.Println(err)
    return
  }
  // Close the file
  fileWriter.Close()
}
