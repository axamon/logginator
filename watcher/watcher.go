package watcher

import (
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sync"

	"github.com/fsnotify/fsnotify"
)

//dichiara il file di appoggio a livello globale
var m sync.RWMutex
var gobfile = "mappafile.db"
var wg sync.WaitGroup
var listafile map[string]bool
var Chanfilezip = make(chan string, 2)

//var chanfilezip chan string

//crea set come mappa booleana e le attribuisce il metodo mutex per lockarla

func init() {

	//se il file di apppoggio non esiste lo crea
	if _, err := os.Stat(gobfile); os.IsNotExist(err) {
		fmt.Println("creo file", gobfile)
		f, err := os.Create(gobfile)
		if err != nil {
			os.Exit(1)
		}
		defer f.Close()
		//salva set nella mappa per inizializzarla,
		//senza questo non funziona un cazzo
		//ecco perchè è nella func init

		listafile = make(map[string]bool)
		savemapgob(listafile, &m)
	}

}

type mufile struct {
	gobfile string
	mu      sync.RWMutex
}

func savemapgob(data map[string]bool, m *sync.RWMutex) {
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
	return
}

func readmapfromgob(gobfile string) (res map[string]bool) {
	var data map[string]bool
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

//Watchdir prende i nuovi file e li processa
func Watchdir(dir string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()
	err = watcher.Add(dir)
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case event := <-watcher.Events:
			//log.Println("event:", event)
			if event.Op&fsnotify.Create == fsnotify.Create {
				wg.Add(1)
				fmt.Println(event.Name)
				filename := string(event.Name)
				go Elabora(filename)
			}
		case err := <-watcher.Errors:
			log.Println("error:", err)
			wg.Done()
			os.Exit(1)
		}
	}
}

func Elabora(filename string) {

	//notifica la chiusura della Go routine
	defer wg.Done()

	//verifica che il file sia zippato leggendo l'estensione
	zippato, err := regexp.MatchString("^.*gz$", filename)
	if err != nil {
		log.Fatal(err, "non so regexare un fava")
	}
	if zippato == true {

		//finchè il file non si apre senza errori ciclalo, poi chiudilo ed esci
		for {
			f, err := os.Open(filename)
			defer f.Close()
			if err == nil {
				break
			}
		}

		//esegue md5sum del file intero
		md5sum := Md5Sum(filename)

		//carica la mappa dal file
		listafile = readmapfromgob(gobfile)

		//Verifica se l'hash esiste già nella mappa e se esiste già esce
		for hash := range listafile {
			if hash == md5sum { //file già esistente in mappa
				fmt.Println("Già visto", hash)
				//fmt.Println(listafile)
				return
			}
		}

		//Se non esiste invece...

		//In ogni caso aggiungi md5sum alla mappa
		//locka la mappa in scrittura
		m.Lock()

		//incrementa di un elemento la mappa
		listafile[md5sum] = true
		fmt.Println("aggiorno mappa")
		//unlocka la mappa
		m.Unlock()

		//salva la mappa sul file gobfile
		go savemapgob(listafile, &m)

		//comunica il nuovo file sul canale
		Chanfilezip <- filename
		//fmt.Println(filename, "dopo del canale")

		//fmt.Println(filename)
	}
	return //fine della go routine
}
