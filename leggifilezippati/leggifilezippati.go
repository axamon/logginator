//Package leggifilezippati permette di leggere file in formago gzip
package leggifilezippati

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"encoding/csv"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
)

const (
	gobfile string = "mappafile.db"
)

//Md5Sum restitus the md5sum of a file in hexadecimal
func Md5Sum(filename string) (result string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	result = hex.EncodeToString(h.Sum(nil))
	return result
}

//salva su disco una mappa
func savemapgob(data map[string]string, m *sync.RWMutex) {
	m.Lock()
	dataFile, err := os.Create(gobfile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	dataEncoder := gob.NewEncoder(dataFile)
	dataEncoder.Encode(data)
	m.Unlock()
	dataFile.Close()
}

//legge da disco una mappa
func readmapfromgob(gobfile string) (res map[string]string) {
	var data map[string]string
	dataFile, err := os.Open(gobfile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	dataDecoder := gob.NewDecoder(dataFile)
	err = dataDecoder.Decode(&data)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	dataFile.Close()
	return data
}

//salvaree lista file su disco con percorso assoluto + md5sum

type filetrovati struct {
	nomefile map[string]bool
	mux      sync.RWMutex
}

//Contafileindir prende i nuovi file e li mette in un canale
func (f *filetrovati) Contafileindir(dir string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				//log.Println("event:", event)
				if event.Op&fsnotify.Create == fsnotify.Create {
					if strings.Contains(event.Name, "gz") {
						md5sum := Md5Sum(event.Name)
						f.mux.Lock()
						f.nomefile[md5sum] = true
						f.mux.Unlock()
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
func Leggizip(fileschan chan string, delimiter rune) (filezipchan chan []string) {

	//riceve il nome file dal canale filescan e itera
	for filezippato := range fileschan {
		f, err := os.Open(filezippato)
		if err != nil {
			fmt.Println(err)
		}
		defer f.Close()

		fmt.Println(f)
		//apre il file zippano
		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		defer gr.Close()

		//apre il file csv
		cr := csv.NewReader(gr)
		cr.FieldsPerRecord = -1 //accetta numero di campi variabili
		cr.Comment = '#'        //tutti i record che inziano con # vengono ignorati
		cr.Comma = delimiter    //specifica il delimitatore dei campi
		cr.LazyQuotes = true    //se le "" sono mezze a cazzo non fa nulla

		// per ogni riga del file fino alla fine EOF itera
		for {
			rec, err := cr.Read()
			if err == io.EOF {
				break

			}
			if err != nil {
				fmt.Println(err)
			}
			//invia il singolo record sul canale filezipchan
			filezipchan <- rec
		}
		f.Close()

	}
	//chiude il canale
	close(filezipchan)
	return

}
