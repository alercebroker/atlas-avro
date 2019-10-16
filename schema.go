package main

import (
  "encoding/json"
  "fmt"
  "github.com/khezen/avro"
)

/* Column names: RA,Dec,mag,dmag,x,y,major,minor,phi,det,chi/N,Pvr,Ptr,Pmv,
   Pkn,Pno,Pbn,Pcr,Pxt,Psc,Dup,WPflx,dflx */
func main() {
  schemaBytes := []byte(
    `{
      "namespace": "atlas",
      "type": "record",
      "name": "alert",
      "doc": "avro schema for ATLAS alerts",
      "version": "0.1",
      "fields": [
      {"name": "schemavsn", "type": "string", "doc": "schema version used"},
      {"name": "RA", "type": "double", "doc": "RA"},
      {"name": "Dec", "type": "double", "doc": "Dec"},
      {"name": "mag", "type": "double", "doc": "mag"},
      {"name": "dmag", "type": "double", "doc": "dmag"},
      {"name": "x", "type": "double", "doc": "x"},
      {"name": "y", "type": "double", "doc": "y"},
      {"name": "major", "type": "double", "doc": "major"},
      {"name": "minor", "type": "double", "doc": "minor"},
      {"name": "phi", "type": "double", "doc": "phi"},
      {"name": "det", "type": "double", "doc": "det"},
      {"name": "chi/N", "type": "double", "doc": "chi/N"},
      {"name": "Pvr", "type": "double", "doc": "Pvr"},
      {"name": "Ptr", "type": "double", "doc": "Ptr"},
      {"name": "Pmv", "type": "double", "doc": "Pmv"},
      {"name": "Pkn", "type": "double", "doc": "Pkn"},
      {"name": "Pno", "type": "double", "doc": "Pno"},
      {"name": "Pbn", "type": "double", "doc": "Pbn"},
      {"name": "Pcr", "type": "double", "doc": "Pcr"},
      {"name": "Pxt", "type": "double", "doc": "Pxt"},
      {"name": "Psc", "type": "double", "doc": "Psc"},
      {"name": "Dup", "type": "double", "doc": "Dup"},
      {"name": "WPflx", "type": "double", "doc": "WPflx"},
      {"name": "dflx", "type": "double", "doc": "dflx"},
      {"name": "cutoutScience", "type": ["cutout", "null"], "default": null},
      {"name": "cutoutTemplate", "type": ["cutout", "null"], "default": null},
      {"name": "cutoutDifference", "type": ["cutout", "null"], "default": null}
      ]}`,
  )

  // Unmarshal JSON  bytes to Schema interface
  var anySchema avro.AnySchema
  err := json.Unmarshal(schemaBytes, &anySchema)
  if err != nil {
    panic(err)
  }
  schema := anySchema.Schema()
  // Marshal Schema interface to JSON bytes
  schemaBytes, err = json.Marshal(schema)
  if err != nil {
    panic(err)
  }
  fmt.Println(string(schemaBytes))
}
