package main

//[54 6 102 111 111]
//{27 foo}

import (
  "fmt"
  "github.com/hamba/avro"
  "io/ioutil"
  "os"
  "log"
  "reflect"
)

type SimpleRecord struct {
        A int64  `avro:"a"`
        B string `avro:"b"`
}

func main() {
    schema, err := avro.Parse(`{
        "type": "record",
        "name": "simple",
        "namespace": "hamba",
        "fields" : [
            {"name": "a", "type": "long"},
            {"name": "b", "type": "string"}
        ]
    }`)
    if err != nil {
        log.Fatal(err)
    }

    in := SimpleRecord{A: 27, B: "foo"}

    data, err := avro.Marshal(schema, in)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println(reflect.TypeOf(data))
    fmt.Println(data)

    mode := int(0644)
    permissions := os.FileMode(mode)
    err = ioutil.WriteFile("file.avro", data, permissions)
    if err != nil {
        log.Fatal(err)
    }
}
