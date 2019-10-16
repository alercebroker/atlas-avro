package main

import (
  "bufio"
  "encoding/csv"
  "encoding/json"
  "fmt"
  "github.com/hamba/avro"
  // "github.com/khezen/avro"
  "io"
  "io/ioutil"
  "log"
  "os"
)

//func readCSVFile(filename string) io.Reader, err {
//	return (x + y) / 2
//}

/*csvfile, err := os.Open("input.csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// Parse the file
	r := csv.NewReader(csvfile)
	//r := csv.NewReader(bufio.NewReader(csvfile))

	// Iterate through the records
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Question: %s Answer %s\n", record[0], record[1])
	}*/
  type Record struct {
    RA double `avro:"RA"`
    Dec double `avro:"Dec"`
    mag double `avro:"mag"`
    dmag double `avro:"dmag"`
    x double `avro:"x"`
    y double `avro:"y"`
    major double `avro:"major"`
    minor double `avro:"minor"`
    phi double `avro:"phi"`
    det double `avro:"det"`
    chi/N double `avro:"chi/N"`
    Pvr double `avro:"Pvr"`
    Ptr double `avro:"Ptr"`
    Pmv double `avro:"Pmv"`
    Pkn double `avro:"Pkn"`
    Pno double `avro:"Pno"`
    Pbn double `avro:"Pbn"`
    Pcr double `avro:"Pcr"`
    Pxt double `avro:"Pxt"`
    Psc double `avro:"Psc"`
    Dup double `avro:"Dup"`
    WPflx double `avro:"WPflx"`
    dflx double `avro:"dflx"`
  }

func main() {
  // Read AVRO file as string
  schemaString, err := ioutil.ReadFile("alert.avsc")

  //mainSchema, err := avro.ParseSchemaFile("alert.avsc")

  schema, err := avro.Parse(string(schemaString))
  if err != nil {
    log.Fatal(err)
  }

  in := Record{
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
    chi/N: 0.23,
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
    dflx: 3.4,}

  data, err := avro.Marshal(schema, in)
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(data)
  out := Record{}
  err = avro.Unmarshal(schema, data, &out)
  if err != nil {
    log.Fatal(err)
  }
  fmt.Println(out)

  // Unmarshal JSON  bytes to Schema interface
/*  var anySchema avro.AnySchema
  err := json.Unmarshal(mainSchema, &anySchema)
  if err != nil {
    panic(err)
  }
  schema := anySchema.Schema()*/
  // Marshal Schema interface to JSON bytes
  //mainSchema, err = json.Marshal(mainSchema)
  //if err != nil {
  //  panic(err)
  //}
  //fmt.Println(string(mainSchema))
}
