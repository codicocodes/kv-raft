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
		fileName := time.Now().Format("2006-01-02") + "." + time.Now().Format("15:04.000000") + ".logger"
		filePath := currPath + "/logs/" + fileName
		file, _ = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
	if file == nil {
		return
	}
	fileName := file.Name()
  msg := fmt.Sprintf("Error in log file: %s\n", fileName)
	fmt.Println(msg)
	
	currPath, _ := os.Getwd()
	filePath := currPath + "/logs/" + "erroring.logs" 
	errFile, _ := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	errFile.WriteString(msg)
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

func (rf *Raft) printLastLog(fnName string)  {
	entry := rf.log[len(rf.log) - 1]
	DPrintf("[%s.%d.%d] Last Command In Log [%d|%d]=%d", fnName, rf.currentTerm, rf.me, entry.Index, entry.Term, entry.Command)
}
