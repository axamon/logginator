//logginator

package main

import (
	"fmt"
	"logginator/leggifilezippati"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

func main() {

	//Create waitgroup to manage go routines
	var wg sync.WaitGroup
	wg.Add(1)
	var totbytes int
	var numfile int

	servizi := make(map[string]int)

	fileschan := make(chan string)     //canale con i file presenti in dir
	filezipchan := make(chan []string) //canale con le righe del file decompresse
	//strchan := make(chan string)

	if len(os.Args) != 3 {
		fmt.Println("Verifica i parametri")
		os.Exit(1)
	}
	dir := os.Args[1] //ad esempio  c:\Projects\Go\src\leggifilezippati
	status, _ := strconv.Atoi(os.Args[2])

	go leggifilezippati.Contafileindir(dir, fileschan)
	go leggifilezippati.Leggizip(fileschan, ' ', filezipchan) //passa il channel il nome file e il delimitatore di campo
	go leggifilezippati.Leggizip(fileschan, ' ', filezipchan)
	go func() {
		wg.Done()

		for {
			time.Sleep(10 * time.Second)
			keys := []string{}
			for key := range servizi {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, val := range keys {

				fmt.Println(val, servizi[val])
			}
			fmt.Println()
		}
	}()
	for totale := range filezipchan {
		u, err := url.Parse(totale[1])
		if err != nil {
			//log.Fatal(err)
			continue
		}
		//fmt.Println(totale)
		httpstatus, err := strconv.Atoi(totale[10])
		if err != nil {
			//log.Fatal(err)
			continue
		}
		bytes, err := strconv.Atoi(totale[4])
		if err != nil {
			//log.Fatal(err)
			continue
		}
		totbytes = totbytes + bytes
		if httpstatus >= status {
			numfile++

			//fmt.Println(httpstatus, u.Host, u.Path, totbytes)
			servizi[u.Host] = totbytes
			//data := []byte(totale[0] + totale[1])
			//fmt.Printf("%x", md5.Sum(data))
			//fmt.Println(totale)

		}
		//fmt.Println(numfile)
	}
	wg.Wait()

}
