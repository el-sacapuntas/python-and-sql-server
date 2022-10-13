package main

import (
	"encoding/json"
	"fmt"
	"io"

	//needed for fatal errors
	"log"
	//needed to open json files
	"os"

	"database/sql"
	//needed to unquote datetime values
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

type Timestamp struct {
	time.Time
}

//cts = converted timestamp
func (cts *Timestamp) UnmarshalJSON(ts []byte) error {
	//remove quotation marks, can also use s := strings.Trim(string(ts), "\"")
	s, err := strconv.Unquote(string(ts))
	//s = strings.Replace(s, " UTC", "", 1)
	if err != nil {
		log.Fatal("error removing quotes: ", err.Error())
	}
	//handle cases where no timestamps given. If fatal reqd, replace return with log.Fatal to quit
	if s == "null" || s == `""` {
		return nil
	}
	//use .99 for decimal seconds to remove trailing zeroes from fractional time
	layout := "2006-01-02 15:04:05.99 MST"
	tsParsed, err := time.Parse(layout, s)
	//pass value
	*cts = Timestamp{tsParsed}
	return err
}

type Trade struct {
	Tradedate      int       `json:"tradedate,string"`
	EventTimestamp Timestamp `json:"event_timestamp"`
	InstrumentID   string    `json:"instrument_id"`
}

type ValueData struct {
	Tradedate     int       `json:"tradedate,string"`
	InstrumentID  string    `json:"instrument_id"`
	WhenTimestamp Timestamp `json:"when_timestamp"`
	Gamma         float64   `json:"gamma"`
	Vega          float64   `json:"vega"`
	Theta         float64   `json:"theta"`
}

func writeValueData() (int, error) {
	valueDataFile, err := os.Open("data/valuedata.json")
	if err != nil {
		log.Fatal("Cannot open json file: ", err.Error())
	}
	input := json.NewDecoder(valueDataFile)

	valuedatum := make([]ValueData, 0)

	for {
		var valueData ValueData

		err := input.Decode(&valueData)
		if err == io.EOF {
			//all lines have been read, so break from loop
			break
		}
		if err != nil {
			log.Fatal("Fatal error with decoding input file: ", err.Error())
			return -1, err
		}
		valuedatum = append(valuedatum, valueData)
	}
	db, err := sql.Open("mssql", "server=localhost;user id=xxxx;password=xxxx;database=mako")
	if err != nil {
		fmt.Println("error connecting: ", err)
		return -1, err
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		fmt.Println("error pinging db: ")
		return -1, err
	}

	_, err = db.Exec("use mako")
	if err != nil {
		fmt.Println("cannot use db: ", err.Error())
		return -1, err
	}
	rowErrors := 0
	stmt, err := db.Prepare("INSERT INTO dbo.value_data (tradedate, instrument_id, when_timestamp, gamma, vega, theta) VALUES(?,?,?,?,?,?)")
	if err != nil {
		fmt.Println("failure to repare db: ", err.Error())
		return -1, err
	}
	for _, vd := range valuedatum {
		//see detailed examples at https://github.com/microsoft/sql-server-samples/blob/master/samples/tutorials/go/crud.go
		// SQL server datetime does not accept timezone (UTC) so format to remove
		//alternative method:
		// tsql := fmt.Sprintf("INSERT INTO dbo.value_data (tradedate, instrument_id, when_timestamp, gamma, vega, theta) VALUES ('%d','%s','%s','%2.30f','%2.30f','%2.30f');",
		// 	vd.Tradedate, vd.InstrumentID, vd.WhenTimestamp.Format("2006-01-02 15:04:05.99"), vd.Gamma, vd.Vega, vd.Theta)
		_, err := stmt.Exec(vd.Tradedate, vd.InstrumentID, vd.WhenTimestamp.Format("2006-01-02 15:04:05.99"), vd.Gamma, vd.Vega, vd.Theta)
		if err != nil {
			rowErrors += 1
		}
	}
	return rowErrors, nil
}

func writeTrades() (int, error) {
	tradesInputFile, err := os.Open("data/trades.json")
	if err != nil {
		log.Fatal("Cannot open json file: ", err.Error())
	}

	input := json.NewDecoder(tradesInputFile)

	trades := make([]Trade, 0)

	for {
		var trade Trade

		err := input.Decode(&trade)
		if err == io.EOF {
			//all lines have been read, so break from loop
			break
		}
		if err != nil {
			log.Fatal("Fatal error with decoding input file: ", err.Error())
		}
		trades = append(trades, trade)
	}

	db, err := sql.Open("mssql", "server=localhost;user id=xxxx;password=xxxx;database=mako")
	if err != nil {
		fmt.Println("error connecting: ", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("use mako")
	if err != nil {
		log.Fatal("cannot use db: ", err.Error())
	}
	rowErrors := 0

	stmt, err := db.Prepare("INSERT INTO dbo.trades (tradedate, event_timestamp, instrument_id) VALUES(?,?,?)")
	if err != nil {
		log.Fatal("error preparing db: ", err.Error())
	}

	for _, trade := range trades {
		//see detailed examples at https://github.com/microsoft/sql-server-samples/blob/master/samples/tutorials/go/crud.go
		// SQL server datetime does not accept timezone (UTC) so format to remove
		// alternative method:
		// tsql := fmt.Sprintf("INSERT INTO dbo.trades (tradedate, event_timestamp, instrument_id) VALUES ('%d','%s','%s');",
		// 	trade.Tradedate, trade.EventTimestamp.Format("2006-01-02 15:04:05.99"), trade.InstrumentID)
		// _, err := db.Exec(tsql)

		_, err := stmt.Exec(trade.Tradedate, trade.EventTimestamp.Format("2006-01-02 15:04:05.99"), trade.InstrumentID)

		if err != nil {
			rowErrors += 1
		}
	}
	return rowErrors, nil
}

func main() {
	tradeErrors, tradeErr := writeTrades()
	if tradeErr != nil {
		fmt.Println("error writing trades to sql: ", tradeErr.Error())
	} else {
		fmt.Println("successfully updated trades table with ", tradeErrors, " errors")
	}

	vdErrors, vdErr := writeValueData()
	if vdErr != nil {
		fmt.Println("error writing trades to sql: ", vdErr.Error())
	} else {
		fmt.Println("successfully updated value data table with ", vdErrors, " errors")
	}

	if tradeErr != nil && vdErr != nil {
		fmt.Println("finished all with errors")
	} else {
		fmt.Println("finished with no errors")
	}
}
