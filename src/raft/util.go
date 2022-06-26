package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
const Debug = false

var file *os.File

func EnableLogger() {
	if file == nil {
		curPath, _ := os.Getwd()
		filePath := curPath + "/logs/" + time.Now().Format("2006-01-02") + "." + time.Now().Format("15:04") + ".logger"
		file, _ = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
}

func Write(str string) {
	_, err := file.WriteString(str)
	if err != nil {
      panic(err)
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
  EnableLogger()
  msg := fmt.Sprintf(format, a...)
	if Debug {
		log.Println(msg)
	}
  Write(msg)
	return
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
