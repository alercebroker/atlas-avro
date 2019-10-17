package main

import (
  "bytes"
  "fmt"
  "gopkg.in/avro.v0"
  "log"
  "os"
)

type AtlasRecord struct {
        A int64  `avro:"a"`
        B string `avro:"b"`
}

type AtlasRecord struct {
  schemavsn string `avro:"schemavsn"`
  RA  float64 `avro:"RA"`
  Dec  float64 `avro:"Dec"`
  mag  float64 `avro:"mag"`
  dmag  float64 `avro:"dmag"`
  x  float64 `avro:"x"`
  y  float64 `avro:"y"`
  major  float64 `avro:"major"`
  minor  float64 `avro:"minor"`
  phi  float64 `avro:"phi"`
  det  float64 `avro:"det"`
  chi/N  float64 `avro:"chi/N"`
  Pvr  float64 `avro:"Pvr"`
  Ptr  float64 `avro:"Ptr"`
  Pmv  float64 `avro:"Pmv"`
  Pkn  float64 `avro:"Pkn"`
  Pno  float64 `avro:"Pno"`
  Pbn  float64 `avro:"Pbn"`
  Pcr  float64 `avro:"Pcr"`
  Pxt  float64 `avro:"Pxt"`
  Psc  float64 `avro:"Psc"`
  Dup  float64 `avro:"Dup"`
  WPflx  float64 `avro:"WPflx"`
  dflx  float64 `avro:"dflx"`
  cutoutScience *Cutout `avro:"cutoutScience"`
  cutoutTemplate *Cutout `avro:"cutoutTemplate"`
  cutoutDifference *Cutout `avro:"cutoutDifference"`
}

type Cutout struct {
  fileName string
  stampData bytes //fits.gz
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
