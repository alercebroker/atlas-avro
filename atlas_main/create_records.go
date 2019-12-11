package main

import (
  "io/ioutil"
  "log"
  "strconv"
)

type Cutout struct {
  FileName string `avro:"fileName"`
  StampData []byte `avro:"stampData"` // bytes
}

type AtlasRecord struct {
  SchemaVersion string `avro:"schemavsn"`
  RA float64 `avro:"RA"`
  Dec float64 `avro:"Dec"`
  Mag float64 `avro:"mag"`
  Dmag float64 `avro:"dmag"`
  X float64 `avro:"x"`
  Y float64 `avro:"y"`
  Major float64 `avro:"major"`
  Minor float64 `avro:"minor"`
  Phi float64 `avro:"phi"`
  Det float64 `avro:"det"`
  ChiN float64 `avro:"chi/N"`
  Pvr float64 `avro:"Pvr"`
  Ptr float64 `avro:"Ptr"`
  Pmv float64 `avro:"Pmv"`
  Pkn float64 `avro:"Pkn"`
  Pno float64 `avro:"Pno"`
  Pbn float64 `avro:"Pbn"`
  Pcr float64 `avro:"Pcr"`
  Pxt float64 `avro:"Pxt"`
  Psc float64 `avro:"Psc"`
  Dup float64 `avro:"Dup"`
  WPflx float64 `avro:"WPflx"`
  Dflx float64 `avro:"dflx"`
  Candid string `avro:"candid"`
  ObjectID string `avro:"objectID"`
  Mjd float64 `avro:"mjd"`
  CutoutScience *Cutout `avro:"cutoutScience"`
  CutoutTemplate *Cutout `avro:"cutoutTemplate"`
  CutoutDifference *Cutout `avro:"cutoutDifference"`
}

func createCutout(directory string, cutout_file_name string) *Cutout {
  // Read stamp data
  cutout_data, err :=  ioutil.ReadFile(directory + cutout_file_name)
  if err != nil {
    log.Fatal(err)
  }
  // Create cutout object
  p_cutout := &Cutout{
    FileName: cutout_file_name,
    StampData: cutout_data,
  }
  return p_cutout
}

func createRecord(data []interface{}) *AtlasRecord {
  SchemaVersion := string(data[0].(string))
  RA, _ := strconv.ParseFloat(data[1].(string), 64)
  Dec, _ := strconv.ParseFloat(data[2].(string), 64)
  Mag, _ := strconv.ParseFloat(data[3].(string), 64)
  Dmag, _ := strconv.ParseFloat(data[4].(string), 64)
  X, _ := strconv.ParseFloat(data[5].(string), 64)
  Y, _ := strconv.ParseFloat(data[6].(string), 64)
  Major, _ := strconv.ParseFloat(data[7].(string), 64)
  Minor, _ := strconv.ParseFloat(data[8].(string), 64)
  Phi, _ := strconv.ParseFloat(data[9].(string), 64)
  Det, _ := strconv.ParseFloat(data[10].(string), 64)
  ChiN, _ := strconv.ParseFloat(data[11].(string), 64)
  Pvr, _ := strconv.ParseFloat(data[12].(string), 64)
  Ptr, _ := strconv.ParseFloat(data[13].(string), 64)
  Pmv, _ := strconv.ParseFloat(data[14].(string), 64)
  Pkn, _ := strconv.ParseFloat(data[15].(string), 64)
  Pno, _ := strconv.ParseFloat(data[16].(string), 64)
  Pbn, _ := strconv.ParseFloat(data[17].(string), 64)
  Pxt, _ := strconv.ParseFloat(data[18].(string), 64)
  Pcr, _ := strconv.ParseFloat(data[19].(string), 64)
  Psc, _ := strconv.ParseFloat(data[20].(string), 64)
  Dup, _ := strconv.ParseFloat(data[21].(string), 64)
  WPflx, _ := strconv.ParseFloat(data[22].(string), 64)
  Dflx, _ := strconv.ParseFloat(data[23].(string), 64)
  Candid := string(data[24].(string))
  ObjectID := string(data[25].(string))
  Mjd, _ := strconv.ParseFloat(data[26].(string), 64)
  CutoutScience := data[27].(*Cutout)
  CutoutTemplate := data[28].(*Cutout)
  CutoutDifference := data[29].(*Cutout)
  atlas_record := AtlasRecord{
    SchemaVersion: SchemaVersion,
    RA: RA,
    Dec: Dec,
    Mag: Mag,
    Dmag: Dmag,
    X: X,
    Y: Y,
    Major: Major,
    Minor: Minor,
    Phi: Phi,
    Det: Det,
    ChiN: ChiN,
    Pvr: Pvr,
    Ptr: Ptr,
    Pmv: Pmv,
    Pkn: Pkn,
    Pno: Pno,
    Pbn: Pbn,
    Pxt: Pxt,
    Pcr: Pcr,
    Psc: Psc,
    Dup: Dup,
    WPflx: WPflx,
    Dflx: Dflx,
    Candid: Candid,
    ObjectID: ObjectID,
    Mjd: Mjd,
    CutoutScience: CutoutScience,
    CutoutTemplate: CutoutTemplate,
    CutoutDifference: CutoutDifference,
  }
  return &atlas_record
}
