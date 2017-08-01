//Package leggifilezippati permette di leggere file in formago gzip
package leggifilezippati

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
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
						f.mux.Lock()
						f.nomefile[event.Name] = true
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
func Leggizip(fileschan chan string, delimiter rune, filezipchan chan []string) {

	//ricece il nome file dal canale filescan e itera
	for filezippato := range fileschan {
		f, err := os.Open(filezippato)
		if err != nil {
			//fmt.Println(err)
			//fmt.Println("SpeedyGonzales")
			//se il file non può essere letto lo inserisce nuovamente nel canale
			fileschan <- filezippato
			continue
		}
		defer f.Close()

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

}
