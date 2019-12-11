package main

import (
  "bytes"
  "fmt"
  "gopkg.in/avro.v0"
  "io/ioutil"
  "log"
  "os"
  "path/filepath"
  "strings"
  "time"
)

// Open and return file
func openFile(fileName string) *os.File {
  // Open file
  file, err := os.Open(fileName)
  if err != nil {
    log.Fatal(err)
  }
  return file
}

func main() {
  // Load the configuration file
  configuration, err := loadConfiguration("")
  if err != nil {
    fmt.Println(err)
  }
  // Parse the schema file
  schema, err := avro.ParseSchemaFile(configuration.SchemaFile)
  if err != nil {
    log.Fatal(err)
  }
  // Get initial time
  start := time.Now()
  // Open directory
  directory := configuration.DataDirectory
  // Extension of files that contain the alert information
  info_extension := ".info"
  // Look for all the info files
  info_files, err := filepath.Glob(strings.Join([]string{directory, "*", info_extension}, ""))
  if err != nil {
    log.Fatal(err)
  }
  // String array to store candids
  candids := []string{}
  // For every info file
  for _, info_file := range info_files {
    // Get the file's base name (file name including the extension)
    base_name := filepath.Base(info_file)
    // Leave just the name (candid)
    candid := strings.TrimSuffix(base_name, info_extension)
    candids = append(candids, candid)
  }
  // For each candid
  for _, candid := range candids {
    // Get full path to alert info file
    alert := (strings.Join([]string{directory, candid, info_extension}, ""))
    // Read the alert information
    content, _ := ioutil.ReadFile(alert)
    // Put the contents in an array
    contents := strings.Fields(string(content))
    // Begin by adding the schema version
    schema_version := "0.1"
    alert_data := []interface{}{schema_version}
    // Put the contents of the file in the data of the alert
    for _, element := range contents {
      alert_data = append(alert_data, element)
    }
    // Cutout extensions
    template_file_extension := "_tstamp.fits"
    science_file_extension := "_istamp.fits"
    difference_file_extension := "_dstamp.fits"
    // Generate cutouts
    p_cutoutTemplate := createCutout(directory, candid + template_file_extension)
    p_cutoutScience := createCutout(directory, candid + science_file_extension)
    p_cutoutDifference := createCutout(directory, candid + difference_file_extension)
    // and append them
    alert_data = append(alert_data, p_cutoutScience, p_cutoutTemplate, p_cutoutDifference)
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
    // Instantiate struct
    atlas_record := createRecord(alert_data)
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
  elapsed := time.Since(start)
  log.Printf("Processing took %s", elapsed)
}
