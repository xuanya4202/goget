package main

import (
//	"flag"
	"fmt"
//	"github.com/aws/aws-sdk-go/aws"
//	"github.com/aws/aws-sdk-go/service/s3"
	"os"
//	"s3client"
	"strings"
	"sync"
//	"sync/atomic"
//	"container/list"
//	"time"
	"io"
	"net/http"
	"bufio"
	"bytes"
	"log"
//	"math/rand"
)
var Info *log.Logger
//var IP = "127.0.0.1:8080"
var IP = "hjl.test.dnion.com"
//var IP = "101.71.7.13"
//var IP = [...]string{0:"101.71.7.12",1:"101.71.7.13",2:"101.71.7.14",3:"101.71.7.15"}

type Message struct{
	value []byte
	leng int
}

func GetUrl(ObjectName string, DirNum *sync.WaitGroup){
//	r := rand.New(rand.NewSource(time.Now().UnixNano()))
//	d := r.Intn(3)
//	ip :=IP[d]
	ip := IP

	client := &http.Client{}
	url := "http://"+ip+"/"+ObjectName
	fmt.Println(" url:", url)
	req,err := http.NewRequest("GET", url, nil)
	if err != nil{
		fmt.Println(" http get failed! %v", err)
		return
	}
	resp,err := client.Do(req)

	//url := "http://"+IP+"/"+ObjectName
	//resp,err := http.Get(url)
	if err != nil{
		//fmt.Println(" http get failed! %v", err)
		Info.Println(" url get failed! %v", err)
		return
	}
	defer resp.Body.Close()
	
	fmt.Println(" url11111111:", url)
//	var CToDisk = make(chan Message, 20)
//		
//	go func(){
//		DirNum.Add(1)
//		defer DirNum.Done()
//
//		//cdir,_ := os.Getwd()	
//		cdir := "/cache/data/9JAJNU0EWATMQYCPL1CE_74/hjl-test" 
//		s := strings.LastIndex(ObjectName, "/")
//		dir := string([]byte(ObjectName)[0:s])
//		err =os.MkdirAll(cdir+"/test/"+dir, os.ModePerm)
//		if err != nil{
//			panic(err)
//			//fmt.Println(err)
//			Info.Println(" url mkdir failed! ", err)
//			return
//		}
//		ObjFile,err := os.OpenFile(cdir+"/test/"+ObjectName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
//		if err != nil{
//			panic(err)
//		}
//			
//		defer ObjFile.Close()
//
//		for{
//			v,ok := <-CToDisk
//			if !ok{
//				break
//			}
//			//fmt.Println("value leng:", v.leng)
//			ObjFile.Write(v.value[0:v.leng])
//		}
//	}()
//	
	cdir := "/cache/data/9JAJNU0EWATMQYCPL1CE_74/hjl-test" 
	s := strings.LastIndex(ObjectName, "/")
	dir := string([]byte(ObjectName)[0:s])
	err =os.MkdirAll(cdir+"/test/"+dir, os.ModePerm)
	if err != nil{
		panic(err)
		//fmt.Println(err)
		Info.Println(" url mkdir failed! ", err)
		return
	}
	ObjFile,err := os.OpenFile(cdir+"/test/"+ObjectName+".bak", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil{
		panic(err)
	}
		
	defer ObjFile.Close()

	var body []byte = make([]byte, 1024*128)
	for{
//		var MessageV Message
//		MessageV.value = make([]byte, 1024*128*8)
//		bodyLen,readErr := resp.Body.Read(MessageV.value)
		bodyLen,readErr := resp.Body.Read(body)
		if readErr != nil{
			if readErr == io.EOF {
				fmt.Println("read resp body eof! ", readErr)
				if bodyLen != 0{
					ObjFile.Write(body[0:bodyLen])
//					MessageV.leng = bodyLen
//					CToDisk <- MessageV
				}
				//return
				break
			}

			Info.Println(" url read resp failed! ", ObjectName,  " err:",readErr)
			fmt.Println("read resp body failed! ", readErr)
			//return
			break
		}else{
			fmt.Println("message leng:", bodyLen)
//			MessageV.leng = bodyLen
//			CToDisk <- MessageV
			ObjFile.Write(body[0:bodyLen])
			//fmt.Println("read OK!")
		}
	}
//	close(CToDisk)
//	ObjFile.Close()
//	err = os.Rename(cdir+"/test/"+ObjectName+".bak", cdir+"/test/"+ObjectName)
//	if err != nil{
//		panic(err)
//	}
}

func SelectFile(DirNum *sync.WaitGroup, PrefixCh chan string, DoneCh chan string, Num int){
	for x := 0; x < Num; x++{
		go func() {
			for {
				select {
					case Prefix, ok := <-PrefixCh:
						if !ok {
							return
						}
						GetUrl(Prefix, DirNum)
						DirNum.Done()
					case <-DoneCh:
						return
				}
			}
		}()
	}
}

func main() {
	var DirNum sync.WaitGroup
	var PrefixCh = make(chan string, 1000000)
	//var MaxGoCh = make(chan string, 5000)
	var DoneCh = make(chan string)
	filename := "urllist"
	logname := "log"	
	logFile,err := os.Create(logname)
	defer logFile.Close()
	if err != nil{
		fmt.Println("create log failed!")
		return
	}
	Info = log.New(logFile, "[Info]", log.Llongfile)

	SelectFile(&DirNum, PrefixCh, DoneCh, 64)
	ObjFile,err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil{
		panic(err)
	}

	rd := bufio.NewReader(ObjFile)
	for{
		line,err := rd.ReadString('\n')
		if err != nil || io.EOF == err{
			break
		}
		//fmt.Println(line)
		leng := bytes.Count([]byte(line),nil)-1 
		PrefixCh <-string([]byte(line)[0:leng-1])

		DirNum.Add(1)
		//for{
		//	if(len(PrefixCh) < 200){
		//		break;
		//	}
		//	time.Sleep(1000*1000)
		//}
	}
	ObjFile.Close()
	
	DirNum.Wait()
	DoneCh <-""
}
