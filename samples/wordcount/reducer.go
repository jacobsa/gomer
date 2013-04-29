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
	"fmt"
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
