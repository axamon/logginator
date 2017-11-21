//logginator dev

package main

import (
	"fmt"
	"logginator/leggifilezippati"
	"logginator/watcher"
)

var Chanfilezip = make(chan string, 2) //Canale usato da watcher

func main() {

	//Create waitgroup to manage go routines
	// var wg sync.WaitGroup

	// var totbytes int
	// var numfile int

	// servizi := make(map[string]int)    //mappa dei servizi e della banda servita
	filezipchan := make(chan []string) //canale con le righe del file decompresse

	// if len(os.Args) != 3 { //se i parametri passati non sono 2 allora esce con errore
	// 	fmt.Println("Verifica i parametri")
	// 	os.Exit(1)
	// }
	// dir := os.Args[1] //ad esempio  c:\Projects\Go\src\leggifilezippati
	dir := "c:\\test2"
	//trasforma lo status http da stringa a intero
	// status, _ := strconv.Atoi(os.Args[2])

	//Adds one to the wg counter
	// wg.Add(1)

	//abilita un watcher sulla directory indicata dir e scrive ogni nuovo nome file in fileschan
	go watcher.Watchdir(dir)
	go leggifilezippati.Leggizip(Chanfilezip, ' ') //passa il channel il nome file e il delimitatore di campo
	//go leggifilezippati.Leggizip(chanfilezip, ' ', filezipchan)

	for {
		if len(Chanfilezip) != 0 {
			fmt.Println(len(Chanfilezip))
			break
		}
	}
	for {
		select {

		case x := <-filezipchan:
			fmt.Println(x)
		case <-Chanfilezip:
			fmt.Println(<-Chanfilezip)
		}
	}

}
