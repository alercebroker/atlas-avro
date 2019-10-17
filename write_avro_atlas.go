package main

import (
  "bytes"
  "fmt"
  "gopkg.in/avro.v0"
  "log"
  "os"
)

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
  chi  float64 `avro:"chi/N"`
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
  stampData []byte // bytes //fits.gz
}

func main() {
  // Create cutouts
  var p_cutoutScience *Cutout
  p_cutoutScience = new(Cutout)
  p_cutoutScience.fileName = "candid820127160015015005_pid820127160015_targ_sci.fits.gz"
  p_cutoutScience.stampData = []byte{0x61, 0x62, 0x43}

  var p_cutoutTemplate *Cutout
  p_cutoutTemplate = new(Cutout)
  p_cutoutTemplate.fileName = "candid820127160015015005_pid820127160015_targ_tem.fits.gz"
  p_cutoutTemplate.stampData = []byte{0x61, 0x63, 0x10}

  var p_cutoutDifference *Cutout
  p_cutoutDifference = new(Cutout)
  p_cutoutDifference.fileName = "candid820127160015015005_pid820127160015_targ_dif.fits.gz"
  p_cutoutDifference.stampData = []byte{0x21, 0x67, 0x20}

  // Parse the schema file
  schema, err := avro.ParseSchemaFile("alert.avsc")
  if err != nil {
    log.Fatal(err)
  }
  // Open file to write to
  f, err := os.Create("alert.avro")
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
  // Create an ATLAS record
  atlas_record := &AtlasRecord{
    schemavsn: "0.1",
    RA: 261.09578,
    Dec: 45.54479,
    mag: 14.806,
    dmag: 0.18,
    x: 1000.9,
    y: 29.19,
    major: 2.27,
    minor: 1.97,
    phi: 128.2,
    det: 0,
    chi: 0.23,
    Pvr: 999,
    Ptr: 0,
    Pmv: 0,
    Pkn: 0,
    Pno: 0,
    Pbn: 0,
    Pcr: 0,
    Pxt: 0,
    Psc: 0,
    Dup: 1,
    WPflx: 78875.5,
    dflx: 3.4,
    cutoutScience: p_cutoutScience,
    cutoutTemplate: p_cutoutTemplate,
    cutoutDifference: p_cutoutDifference,
  }
  // Write the data to the buffer through datumWriter
  err = datumWriter.Write(atlas_record, encoder)
  if err != nil {
    log.Fatal(err)
  }
  // Create a fileWriter
  fileWriter, err := avro.NewDataFileWriter(f, schema, datumWriter)
  if err != nil {
    log.Fatal(err)
  }
  // fileWriter needs an argument
  err = fileWriter.Write(atlas_record)
  err = fileWriter.Flush()
  if err != nil {
    fmt.Println(err)
    return
  }
  // Close the file
  fileWriter.Close()
}
