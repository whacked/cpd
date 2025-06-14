package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/whacked/yamdb/pkg/demos"
	"gopkg.in/yaml.v3"
)

func main() {
	if !false {

		demos.RunYamlDemo()
		os.Exit(0)

	} else if false {
		fmt.Println("hello")

		source, err := os.ReadFile("tests/example-1.yaml")
		if err != nil {
			fmt.Printf("error reading file: %v\n", err)
			log.Fatal(err)
		}
		var result []map[string]interface{}
		err = yaml.Unmarshal(source, &result)
		if err != nil {
			fmt.Printf("error unmarshalling yaml: %v\n", err)
			log.Fatal(err)
		}
		fmt.Printf("result: %+v\n", result)
		os.Exit(0)

	}

	// Get filepath from args or use default
	filepath := "tests/example-2.jsonl"
	if len(os.Args) > 1 {
		lastArg := os.Args[len(os.Args)-1]
		if strings.HasSuffix(lastArg, ".jsonl") {
			// Check if file exists
			if _, err := os.Stat(lastArg); err != nil {
				fmt.Printf("Error: file %s not found\n", lastArg)
				os.Exit(1)
			}
			filepath = lastArg
		}
	}

	if true {
		demos.RunJsonlDemo(filepath, 0)
	} else {
		demos.RunJsonToYamlDemo(filepath)
	}

	demos.MetaVersionDemo()
}
