package main

import (
  "fmt"
  "log"
  //"io/ioutil"
  "os"
  "path/filepath"
)

func FilePathWalkDir(root string) ([]string, error) {
  var files []string
  err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
    if !info.IsDir() {
      files = append(files, path)
    }
    return nil
  })
  return files, err
}

func main() {
  // Get program arguments
  if len(os.Args) > 1 {
    programArguments := os.Args[1:] // Could be a directory or something else
    // Read files in directory
    directory := programArguments[0]
    // Files is a list of the files in the directory
    files, err := FilePathWalkDir(directory)
    if err != nil {
      panic(err)
    }
    // TODO: Get the headers or find them in the file
    // Read file data
    
    // Create avro files

    // produce to topic
  }else{
    log.Fatal("No arguments given.")
  }
}
