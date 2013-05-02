// Copyright 2013 Aaron Jacobs. All Rights Reserved.
// Author: aaronjjacobs@gmail.com (Aaron Jacobs)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
)

type keyVal struct {
	key []byte
	val []byte
}

func mapEntry(entry keyVal, output chan<- keyVal) {
	// Split the line into words.
	words := bytes.Split(entry.val, []byte(" "))
	for _, word := range words {
		output <- keyVal{word, []byte("1")}
	}
}

func runMapper() {
	// Grab input a line at a time.
	reader := bufio.NewReader(os.Stdin)

	// Start a goroutine that will write output, then quit when it's done.
	doneWriting := make(chan bool)
	output := make(chan keyVal)
	go func() {
		for outputElement := range output {
			fmt.Printf("%s\t%s\n", outputElement.key, outputElement.val)
		}

		doneWriting<- true
	}()

	for {
		// Grab the next line.
		line, err := reader.ReadBytes('\n')

		// Process the bytes, if any.
		//
		// TODO(jacobsa): Support keys in input.
		if len(line) > 1 {
			// Throw away the delimeter.
			if line[len(line)-1] == '\n' {
				line = line[0:len(line)-1]
			}

			key := []byte{}
			val := line
			mapEntry(keyVal{key, val}, output)
		}

		// Did we finish cleanly?
		if err == io.EOF {
			break
		}

		// Did we fail for some other reason?
		if err != nil {
			panic(fmt.Sprintf("ReadBytes: %v", err))
		}
	}

	// Close the channel and wait for output to be flushed.
	close(output)
	<-doneWriting
}
