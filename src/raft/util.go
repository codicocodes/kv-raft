package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
const Debug = false

const LogFile = true

var file *os.File

func EnableLogger() {
	if file == nil {
		currPath, _ := os.Getwd()
		fileName := time.Now().Format("2006-01-02") + "." + time.Now().Format("15:04") + ".logger"
		filePath := currPath + "/logs/" + fileName
		file, _ = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		fmt.Printf("Opened log file: %s\n", fileName)
		DPrintf("--- START OF TEST ---")
	}
}

func Write(str string) {
	_, err := file.WriteString(str)
	if err != nil {
      panic(err)
	}
}

func DPrintClose(){
	fileName := file.Name()
	fmt.Printf("Error in log file: %s\n", fileName)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
  msg := fmt.Sprintf(format, a...)
	if Debug {
		log.Println(msg)
	}
	if LogFile {
		EnableLogger()
		Write(msg)
	}
	return
}

func max(a, b int) int {
    if a < b {
        return b
    }
    return a
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
