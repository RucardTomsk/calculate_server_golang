package logmodule

import (
	"log"
	"os"
	"strings"
	"time"
)

func GetChanal() chan string {
	return make(chan string)
}

func StartLogs(chanal *chan string) {
	f, _ := os.OpenFile("logs/"+time.Now().Format("2006.01.02 15:04:05")+".log", os.O_RDWR|os.O_CREATE, 0666)
	defer f.Close()

	infoLog := log.New(f, "INFO\t", log.Ldate|log.Ltime)
	var LogS strings.Builder
	for logS := range *chanal {
		//fmt.Println("logging")
		LogS.WriteString("\n")
		LogS.WriteString(logS)
	}

	infoLog.Println(LogS.String())
}

func StopLogs(chanal *chan string) {
	close(*chanal)
}
