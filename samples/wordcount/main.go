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
	"flag"
	"log"
	"os"
)

var g_mode = flag.String("mode", "", "\"map\" or \"reduce\"")

func runReducer()

func main() {
	flag.Parse()

	// Run in the appropriate mode.
	switch *g_mode {
	case "map":
		runMapper()
		return
	case "reduce":
		runReducer()
		return
	default:
		log.Fatalf("Invalid --mode: %s", *g_mode)
	}
}
