//Package leggifilezippati permette di leggere file in formago gzip
package leggifilezippati

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/fsnotify/fsnotify"
)

//Contafileindir prende i nuovi file e li mette in un canale
func Contafileindir(dir string, fileschan chan string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				//log.Println("event:", event)
				if event.Op&fsnotify.Create == fsnotify.Create {
					if strings.Contains(event.Name, "gz") {
						fileschan <- event.Name
					}
				}
			case err := <-watcher.Errors:
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(dir)
	if err != nil {
		log.Fatal(err)
	}
	<-done
	close(fileschan)
}

//ReadLine legge un record e lo passa su un altro canale
func ReadLine(fileschan chan string, strchan chan string) {

	for filezippato := range fileschan {
		f, err := os.Open(filezippato)
		if err != nil {
			fileschan <- filezippato
			continue
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		defer gr.Close()

		scanner := bufio.NewScanner(gr)

		scanner.Split(bufio.ScanLines)

		for scanner.Scan() {
			fmt.Println(scanner.Text())
			strchan <- scanner.Text()
		}
	}
	close(strchan)
}

//Leggizip2 è in test
func Leggizip2(fileschan chan string, delimiter rune, filezipchan chan []string) {

	for {
		select {
		case filezippato := <-fileschan:
			f, err := os.Open(filezippato)
			if err != nil {
				//log.Fatal(err)
				fmt.Println("SpeedyGonzales")
				fileschan <- filezippato
				continue
			}
			defer f.Close()

			gr, err := gzip.NewReader(f)
			if err != nil {
				log.Fatal(err)
			}
			defer gr.Close()

			cr := csv.NewReader(gr)
			cr.FieldsPerRecord = -1 //accetta numero di campi variabili
			cr.Comment = '#'
			cr.Comma = delimiter //specifica il delimitatore dei campi
			cr.LazyQuotes = true
			for {
				rec, err := cr.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					fmt.Println(err)
				}
				filezipchan <- rec
			}
			f.Close()
			close(filezipchan)
		}

	}
}

//Leggizip legge un file zippato
func Leggizip(fileschan chan string, delimiter rune, filezipchan chan []string) {

	for filezippato := range fileschan {
		f, err := os.Open(filezippato)
		if err != nil {
			//fmt.Println(err)
			//fmt.Println("SpeedyGonzales")
			fileschan <- filezippato
			continue
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		defer gr.Close()

		cr := csv.NewReader(gr)
		cr.FieldsPerRecord = -1 //accetta numero di campi variabili
		cr.Comment = '#'
		cr.Comma = delimiter //specifica il delimitatore dei campi
		cr.LazyQuotes = true

		for {
			rec, err := cr.Read()
			if err == io.EOF {
				break

			}
			if err != nil {
				fmt.Println(err)
			}
			filezipchan <- rec
		}
		f.Close()

	}
	close(filezipchan)

}
