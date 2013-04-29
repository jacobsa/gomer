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
	"strconv"
)

func reduce(key []byte, vals <-chan []byte, output chan<- []byte) {
	// Parse each incoming number, keeping a running total.
	var total uint64
	for val := range vals {
		n, err := strconv.ParseUint(string(val), 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Illegal value: %s", val))
		}

		total += n
	}

	// Output the total.
	output <- []byte(strconv.FormatUint(total, 10))
}

func runReducer() {
	// Start a goroutine that will write output, then quit when it's done.
	doneWriting := make(chan bool)
	keyedOutput := make(chan string)
	go func() {
		for outputElement := range keyedOutput {
			fmt.Printf("%s\n", outputElement)
		}

		doneWriting<- true
	}()

	// Start a function that groups by key.
	type keyAndValChan struct {
		key []byte
		vals chan []byte
	}

	groupedInput := make(chan keyAndValChan)
	go func() {
		// Process each line.
		reader := bufio.NewReader(os.Stdin)
		var currentGrouping keyAndValChan

		for {
			// Grab the next line.
			line, err := reader.ReadBytes('\n')

			// Process the bytes, if any.
			if len(line) > 0 {
				// Split into key and value.
				elems := bytes.SplitN(line, []byte("\t"), 2)
				if len(elems) != 2 {
					panic(fmt.Sprintf("Invalid line: %s", line))
				}

				key := elems[0]
				val := elems[1]

				// Is this a new key?
				if currentGrouping.key == nil || !bytes.Equal(currentGrouping.key, key) {
					close(currentGrouping.vals)
					currentGrouping.key = key;
					currentGrouping.vals = make(chan []byte)
					groupedInput <- currentGrouping
				}

				// Pass the value.
				currentGrouping.vals <- val
			}

			// Did we finish cleanly?
			if err == io.EOF {
				close(currentGrouping.vals)
				break
			}

			// Did we fail for some other reason?
			if err != nil {
				panic(fmt.Sprintf("ReadBytes: %v", err))
			}
		}

		close(groupedInput)
	}()

	// Reduce each grouped input.
	for elem := range groupedInput {
		// Pump output values.
		unkeyedOutputs := make(chan []byte)
		donePumping := make(chan bool)

		go func() {
			for unkeyedOutput := range unkeyedOutputs {
				keyedOutput <- fmt.Sprintf("%s\t%s", elem.key, unkeyedOutput)
			}
			donePumping <- true
		}()

		// Call the reducer.
		reduce(elem.key, elem.vals, unkeyedOutputs)

		close(unkeyedOutputs)
		<-donePumping
	}

	// Close the channel and wait for output to be flushed.
	close(keyedOutput)
	<-doneWriting
}
