package internal

import (
	"fmt"
	"os"
	"strconv"
)

// DebugLevel controls verbosity of debug output
var DebugLevel = func() int {
	if level, err := strconv.Atoi(os.Getenv("DEBUG_LEVEL")); err == nil {
		return level
	}
	return 0
}()

// DebugLog prints debug messages if the current debug level is sufficient
func DebugLog(format string, args ...interface{}) {
	if DebugLevel > 0 {
		fmt.Printf("[DEBUG] "+format+"\n", args...)
	}
}
