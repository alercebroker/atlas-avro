package main

import (
  "bytes"
  "fmt"
  "gopkg.in/avro.v0"
  "io/ioutil"
  "log"
  "os"
  "path/filepath"
  "strconv"
  "strings"
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
  Pointing string `avro:"pointing"`
  Candid string `avro:"candid"`
  ObjectID string `avro:"objectID"`
  CutoutScience *Cutout `avro:"cutoutScience"`
  CutoutTemplate *Cutout `avro:"cutoutTemplate"`
  CutoutDifference *Cutout `avro:"cutoutDifference"`
}

// Open and return file
func openFile(fileName string) *os.File {
  // Open file
  file, err := os.Open(fileName)
  if err != nil {
    log.Fatal(err)
  }
  return file
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
  Dup, _ := strconv.ParseFloat(data[20].(string), 64)
  Psc, _ := strconv.ParseFloat(data[21].(string), 64)
  WPflx, _ := strconv.ParseFloat(data[22].(string), 64)
  Dflx, _ := strconv.ParseFloat(data[23].(string), 64)
  Pointing := string(data[24].(string))
  Candid := string(data[25].(string))
  ObjectID := string(data[26].(string))
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
    Dup: Dup,
    Psc: Psc,
    WPflx: WPflx,
    Dflx: Dflx,
    Pointing: Pointing,
    Candid: Candid,
    ObjectID: ObjectID,
    CutoutScience: CutoutScience,
    CutoutTemplate: CutoutTemplate,
    CutoutDifference: CutoutDifference,
  }
  return &atlas_record
}

func main() {
  // Open directory
  directory := "/home/daniela/atlas-avro/atlas_data/SJ009S50_02a58789/"
  candids := []string{}
  // Look for all info files
  matches, err := filepath.Glob(directory + "*.info")
  if err != nil {
    log.Fatal(err)
  }
  for _, match := range matches {
    // base_name is the file name including extension
    base_name := filepath.Base(match)
    extension := filepath.Ext(base_name)
    // leave just the name
    name := strings.TrimSuffix(base_name, extension)
    candids = append(candids, name)
  }
  // for each candid
  for _, candid := range candids {
    //structure to hold alert data
    //var data []interface{} //SchemaVersion
    data := []interface{}{"0.1"} //add SchemaVersion
    /*
    data = append(data, "0.1") //SchemaVersion
    */
    //open the info file
    fmt.Println(candid)
    alert := directory + candid + ".info"
    content, _ := ioutil.ReadFile(alert)
    //put the contents in an array
    content_array := strings.Fields(string(content))[1:] //remove first unneeded line
    for i, element := range content_array {
      if (i == 24) {
        data = append(data, content_array[23] + "_" + content_array[24])
      } else {
        data = append(data, element)
      }
    }
    //create cutouts and append them
    science_file_name := directory + candid + "_sciestmp.fits.gz"
    sci_file, err :=  ioutil.ReadFile(science_file_name)
    if err != nil {
      log.Fatal(err)
    }
    template_file_name := directory + candid + "_tempstmp.fits.gz"
    temp_file, err :=  ioutil.ReadFile(template_file_name)
    if err != nil {
      log.Fatal(err)
    }
    difference_file_name := directory + candid + "_diffstmp.fits.gz"
    diff_file, err :=  ioutil.ReadFile(difference_file_name)
    if err != nil {
      log.Fatal(err)
    }
    // Create cutouts
    p_cutoutScience := &Cutout{
      FileName: science_file_name,
      StampData: sci_file,
    }
    p_cutoutTemplate := &Cutout{
      FileName: template_file_name,
      StampData: temp_file,
    }
    p_cutoutDifference := &Cutout{
      FileName: difference_file_name,
      StampData: diff_file,
    }
    data = append(data, p_cutoutScience, p_cutoutTemplate, p_cutoutDifference)
    // Parse the schema file
    schema, err := avro.ParseSchemaFile("/home/daniela/atlas-avro/schema/plain_schema.avsc")
    if err != nil {
      log.Fatal(err)
    }
    // Open file to write to
    f, err := os.Create(candid + ".avro")
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
    //instantiate struct
    atlas_record := createRecord(data)
    //fmt.Println(*r) //"%+v\n",
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
}
