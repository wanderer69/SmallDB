package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"errors"

	db "github.com/wanderer69/SmallDB/public/index"

	"github.com/kabukky/httpscerts"
	"github.com/wanderer69/SmallDB/internal/common"
)

func TLSGenKey(port int) error {
	err := httpscerts.Check("cert.pem", "key.pem")
	if err != nil {
		if (port < 1024) || (port > 49151) {
			return errors.New("port range out")
		}
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		err = httpscerts.Generate("cert.pem", "key.pem", addr)
		if err != nil {
			return err
		}
	}
	return nil
}

type WordList struct {
	State int
}

type WordItem struct {
	Copy int
	Word string
}

type WordListAnswer struct {
	Words []WordItem
}

func SmallDBServer(prefix string, port int) int {
	/*
	   Методы сервиса маленькой дазы банных
	   	Создать базу
	   	Записать в базу
	   	Прочитать запись либо записи из базы
	   	Найти записи в базе по индексу
	   	Удалить запись из базы
	*/

	H_CreateDB := func(w http.ResponseWriter, req *http.Request) {
		// Создать базу
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// body -> struct
		var jc common.JobCreate
		err = json.Unmarshal([]byte(body), &jc)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ???
		sdb := db.InitSmallDB(jc.DB_path)
		sdb.Debug = 0
		if !sdb.Inited {
			fl := []string{}
			for i := range jc.CreateDB.Fields {
				fl = append(fl, jc.CreateDB.Fields[i].Name)
			}
			err = sdb.CreateDB(fl, jc.DB_path)
			if err != nil {
				error_t = fmt.Sprintf("CreateDB error %v\r\n", err)
			} else {
				for i := range jc.CreateDB.Indexes {
					inx := jc.CreateDB.Indexes[i].Fields
					sdb.CreateIndex(inx)
				}
				result = "OK"
			}
		}

		jr := common.JobResult{}
		jr.Type = "create" // type -> create add find read delete
		jr.Result = result
		jr.Error = error_t
		ba, _ := json.MarshalIndent(jr, "", "  ")

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte(ba))
	}

	H_AddDB := func(w http.ResponseWriter, req *http.Request) {
		// Записать в базу
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// body -> struct
		var ja common.JobAdd
		err = json.Unmarshal([]byte(body), &ja)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ???
		sdb := db.InitSmallDB(ja.DB_path)
		sdb.Debug = 0
		rrec := []common.Record{}
		if sdb.Inited {
			err := sdb.OpenDB()
			if err != nil {
				error_t = fmt.Sprintf("Error %v\r\n", err)
			} else {
				for i := range ja.AddRecs {
					mmd := make(map[string]string)
					for j := range ja.AddRecs[i].Rec {
						f := ja.AddRecs[i].Rec[j].Name
						val := ja.AddRecs[i].Rec[j].Value
						mmd[f] = val
					}
					_, num, err := sdb.StoreRecordOnMap(mmd)
					if err != nil {
						error_t = fmt.Sprintf("Error %v\r\n", err)
						break
					} else {
						rec := common.Record{}
						rec.Num = num
						rrec = append(rrec, rec)
					}
				}
				if len(error_t) == 0 {
					result = "OK"
				}
				sdb.CloseData()
			}
		}

		jr := common.JobResult{}
		jr.Type = "add" // type -> create add find read delete
		jr.Result = result
		jr.Error = error_t
		jr.Records = rrec
		ba, _ := json.MarshalIndent(jr, "", "  ")

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte(ba))
	}

	H_FindDB := func(w http.ResponseWriter, req *http.Request) {
		// Найти записи в базе по индексу
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// body -> struct
		var jf common.JobFind
		err = json.Unmarshal([]byte(body), &jf)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ???
		sdb := db.InitSmallDB(jf.DB_path)
		sdb.Debug = 0
		rrec := []common.Record{}
		if sdb.Inited {
			err := sdb.OpenDB()
			if err != nil {
				error_t = fmt.Sprintf("Error %v\r\n", err)
			} else {
				fl := []string{}
				values := []string{}
				for j := range jf.FindRec {
					f := jf.FindRec[j].Name
					val := jf.FindRec[j].Value
					fl = append(fl, f)
					values = append(values, val)
				}
				ds, _, err1 := sdb.FindRecordIndexString(fl, values)
				if err1 != nil {
					error_t = fmt.Sprintf("Error %v\r\n", err1)
				} else {
					for i := range ds {
						rec := common.Record{Num: ds[i].Num, FieldsValue: ds[i].FieldsValue}
						rrec = append(rrec, rec)
					}
				}
				if len(error_t) == 0 {
					result = "OK"
				}
				sdb.CloseData()
			}
		}

		jr := common.JobResult{}
		jr.Type = "find" // type -> create add find read delete
		jr.Result = result
		jr.Error = error_t
		jr.Records = rrec
		ba, _ := json.MarshalIndent(jr, "", "  ")

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte(ba))
	}

	H_ReadDB := func(w http.ResponseWriter, req *http.Request) {
		// Прочитать запись либо записи из базы
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// body -> struct
		var jrd common.JobRead
		err = json.Unmarshal([]byte(body), &jrd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ???
		sdb := db.InitSmallDB(jrd.DB_path)
		sdb.Debug = 0
		rrec := []common.Record{}
		if sdb.Inited {
			err := sdb.OpenDB()
			if err != nil {
				error_t = fmt.Sprintf("Error %v\r\n", err)
			} else {
				ds, _, err1 := sdb.LoadRecord(jrd.NumRec)
				if err1 != nil {
					error_t = fmt.Sprintf("Error %v\r\n", err1)
				} else {
					for i := range ds {
						rec := common.Record{Num: ds[i].Num, FieldsValue: ds[i].FieldsValue}
						rrec = append(rrec, rec)
					}
				}
				if len(error_t) == 0 {
					result = "OK"
				}
				sdb.CloseData()
			}
		}

		jr := common.JobResult{}
		jr.Type = "read" // type -> create add find read delete
		jr.Result = result
		jr.Error = error_t
		jr.Records = rrec
		ba, _ := json.MarshalIndent(jr, "", "  ")

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte(ba))
	}

	H_DeleteDB := func(w http.ResponseWriter, req *http.Request) {
		// Прочитать запись либо записи из базы
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// body -> struct
		var jd common.JobDelete
		err = json.Unmarshal([]byte(body), &jd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ???
		sdb := db.InitSmallDB(jd.DB_path)
		sdb.Debug = 0
		if sdb.Inited {
			err := sdb.OpenDB()
			if err != nil {
				error_t = fmt.Sprintf("Error %v\r\n", err)
			} else {
				_, err1 := sdb.DeleteRecord(jd.NumRec)
				if err1 != nil {
					error_t = fmt.Sprintf("Error %v\r\n", err1)
				}
				if len(error_t) == 0 {
					result = "OK"
				}
				sdb.CloseData()
			}
		}

		jr := common.JobResult{}
		jr.Type = "delete" // type -> create add find read delete
		jr.Result = result
		jr.Error = error_t
		ba, _ := json.MarshalIndent(jr, "", "  ")

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte(ba))
	}

	go func() {
		http.HandleFunc(prefix+"/c/create", H_CreateDB)
		http.HandleFunc(prefix+"/c/add", H_AddDB)
		http.HandleFunc(prefix+"/c/find", H_FindDB)
		http.HandleFunc(prefix+"/c/read", H_ReadDB)
		http.HandleFunc(prefix+"/c/delete", H_DeleteDB)

		prt := fmt.Sprintf(":%d", port)
		http.ListenAndServe(prt, nil)
	}()
	return 1
}
