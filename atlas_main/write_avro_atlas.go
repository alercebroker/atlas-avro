package main

import (
  "bytes"
  "fmt"
  "gopkg.in/avro.v0"
  "io/ioutil"
  "log"
  "os"
)

type Cutout struct {
  FileName string `avro:"fileName"`
  StampData []byte `avro:"stampData"` // bytes
}

type AtlasRecord struct {
  Schemavsn string `avro:"schemavsn"`
  RA  float64 `avro:"RA"`
  Dec  float64 `avro:"Dec"`
  Mag  float64 `avro:"mag"`
  Dmag  float64 `avro:"dmag"`
  X  float64 `avro:"x"`
  Y  float64 `avro:"y"`
  Major  float64 `avro:"major"`
  Minor  float64 `avro:"minor"`
  Phi  float64 `avro:"phi"`
  Det  float64 `avro:"det"`
  Chi  float64 `avro:"chi/N"`
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
  Dflx  float64 `avro:"dflx"`
  CutoutScience *Cutout `avro:"cutoutScience"`
  CutoutTemplate *Cutout `avro:"cutoutTemplate"`
  CutoutDifference *Cutout `avro:"cutoutDifference"`
}

func main() {
  // Read fits.gz file
  sci_file_name := "sci.fits.gz"
  sci_file, err :=  ioutil.ReadFile(sci_file_name)
  if err != nil {
    log.Fatal(err)
  }
  // Create cutouts
  p_cutoutScience := &Cutout{
    FileName: sci_file_name,
    StampData: sci_file,
  }
  p_cutoutTemplate := &Cutout{
    FileName: "tem.fits.gz",
    StampData: []byte{0x31, 0x12, 0x63},
  }

  p_cutoutDifference := &Cutout{
    FileName: "dif.fits.gz",
    StampData: []byte{0x60, 0x73, 0x43},
  }

  // fmt.Println(p_cutoutDifference.stampData)
  // Parse the schema file
  schema, err := avro.ParseSchemaFile("plain_schema.avsc")
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
    Schemavsn: "0.1",
    RA: 261.09578,
    Dec: 45.54479,
    Mag: 14.806,
    Dmag: 0.18,
    X: 1000.9,
    Y: 29.19,
    Major: 2.27,
    Minor: 1.97,
    Phi: 128.2,
    Det: 0,
    Chi: 0.23,
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
    Dflx: 3.4,
    CutoutScience: p_cutoutScience,
    CutoutTemplate: p_cutoutTemplate,
    CutoutDifference: p_cutoutDifference,
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
