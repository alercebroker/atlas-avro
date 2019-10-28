package main

import (
  "fmt"
  "os"
)

func main() {
  // Get program arguments
  if len(os.Args) > 2 {
    programArguments := os.Args[1:] // Could be a directory where the ddc files are.
    // get ddc/other format file

    // get the headers or find them in the file
    // read file data (in chunks?)
    // create avro files

    // produce to topic
  }
  else {
    fmt.Println("No arguments given.")
  }
}
