//go:build published_goals

package main

import (
	"encoding/json"
	"os"
	"runtime/debug"

	_ "github.com/colony-2/jobdb/pkg/jobdb"
	_ "github.com/colony-2/pgjobdb/pkg/pgjobdb/runtime"
)

func main() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		panic("missing build identity")
	}
	if err := json.NewEncoder(os.Stdout).Encode(info); err != nil {
		panic(err)
	}
}
