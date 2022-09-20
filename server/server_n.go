package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"errors"

	. "github.com/wanderer69/SmallDB/v3"

	"github.com/kabukky/httpscerts"
	. "github.com/wanderer69/SmallDB/common"
)

func TLSGenKey(port int) error {
	err := httpscerts.Check("cert.pem", "key.pem")
	if err != nil {
		if (port < 1024) || (port > 49151) {
			return errors.New("Port range out")
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
/*
		// проверяем токен
		bearToken := req.Header.Get("Authorization")

		bearToken_lst := strings.Split(bearToken, " ")
		if len(bearToken_lst) != 2 {
			//fmt.Printf("2\r\n")
			http.Error(w, "Bad length token", http.StatusInternalServerError)
			return
		}
		if bearToken_lst[0] != "Bearer" {
			http.Error(w, "Authorization not bearer", http.StatusInternalServerError)
			return
		}

		accessToken := bearToken_lst[1]

		fmt.Printf("accessToken %v\r\n", accessToken)

		request := &proto.CheckTokenRequest{
			Token: accessToken,
		}
		response, err := client.CheckToken(context.Background(), request)
		if err != nil {
			grpclog.Fatalf("fail to dial: %v", err)
		}

		fmt.Println(response)
		// ищем 
		if len(response.Result) == 0 {
			  // ошибка !
		}
		fmt.Printf("-> %#v\r\n", response)
		// получили сессионный ключ
		session_key := response.SessionKey
*/

		// body -> struct
		var jc JobCreate 
		err = json.Unmarshal([]byte(body), &jc)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ??? 
		sdb := Init_SmallDB(jc.DB_path)
		sdb.Debug = 0
		p_sdb := &sdb
		if !sdb.Inited {
			fl := []string{}
			for i, _ := range jc.CreateDB.Fields {
				fl = append(fl, jc.CreateDB.Fields[i].Name)
			}

//			fmt.Printf("CreateDB ...")
			res := p_sdb.CreateDB(fl, jc.DB_path)
			if res < 0 {
				error_t = fmt.Sprintf("CreateDB error %v\r\n", res)
			} else {
//			fmt.Printf("done\r\n")
				for i, _ := range jc.CreateDB.Indexes {
					inx := jc.CreateDB.Indexes[i].Fields
					p_sdb.CreateIndex(inx)
				}
				result = "OK"
			}
//			fmt.Printf("CreateDB end\r\n")
		}

		jr := JobResult{}
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
		var ja JobAdd 
		err = json.Unmarshal([]byte(body), &ja)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ??? 
		sdb := Init_SmallDB(ja.DB_path)
		sdb.Debug = 0
		p_sdb := &sdb
		rrec := []Record{}
		if sdb.Inited {
			res, err := p_sdb.OpenDB()
			if err != nil {
				error_t = fmt.Sprintf("Error %v\r\n", err)
			} else {
				if res < 0 {
					error_t = fmt.Sprintf("Error %v\r\n", res)
				} else {
					for i, _ := range ja.AddRecs {
						mmd := make(map[string]string)
						for j, _ := range ja.AddRecs[i].Rec {
							f := ja.AddRecs[i].Rec[j].Name
							val := ja.AddRecs[i].Rec[j].Value
							mmd[f] = val
						}
						_, num, err := sdb.Store_record_on_map(mmd)
						if err != nil {
							error_t = fmt.Sprintf("Error %v\r\n", err)
							break
						} else {
							rec := Record{}
							rec.Num = num
							rrec = append(rrec, rec)
						}
					}
					if len(error_t) == 0 {
						result = "OK"
					}
				}
//				fmt.Printf("done.\r\n")
				p_sdb.CloseData()
			}
		}	

		jr := JobResult{}
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
		var jf JobFind 
		err = json.Unmarshal([]byte(body), &jf)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ??? 
		sdb := Init_SmallDB(jf.DB_path)
		sdb.Debug = 0
		p_sdb := &sdb
		rrec := []Record{}
		if sdb.Inited {
			res, err := p_sdb.OpenDB()
			if err != nil {
				error_t = fmt.Sprintf("Error %v\r\n", err)
			} else {
				if res < 0 {
					error_t = fmt.Sprintf("Error %v\r\n", res)
				} else {
					fl := []string{}
					values := []string{}
					for j, _ := range jf.FindRec {
						f := jf.FindRec[j].Name
						val := jf.FindRec[j].Value
						fl = append(fl, f)
						values = append(values, val)
					}
					ds, _, err1 := p_sdb.Find_record_index_string(fl, values)
					if err1 != nil {
						error_t = fmt.Sprintf("Error %v\r\n", err1)
					} else {
						for i, _ := range ds {
							rec := Record{ds[i].Num, ds[i].FieldsValue}
							rrec = append(rrec, rec)
						}
					}
					if len(error_t) == 0 {
						result = "OK"
					}

				}
//				fmt.Printf("done.\r\n")
				p_sdb.CloseData()
			}
		}	

		jr := JobResult{}
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
		var jrd JobRead 
		err = json.Unmarshal([]byte(body), &jrd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ??? 
		sdb := Init_SmallDB(jrd.DB_path)
		sdb.Debug = 0
		p_sdb := &sdb
		rrec := []Record{}
		if sdb.Inited {
			res, err := p_sdb.OpenDB()
			if err != nil {
				error_t = fmt.Sprintf("Error %v\r\n", err)
			} else {
				if res < 0 {
					error_t = fmt.Sprintf("Error %v\r\n", res)
				} else {
					ds, _, err1 := p_sdb.Load_record(jrd.NumRec)
					if err1 != nil {
						error_t = fmt.Sprintf("Error %v\r\n", err1)
					} else {
						for i, _ := range ds {
							rec := Record{ds[i].Num, ds[i].FieldsValue}
							rrec = append(rrec, rec)
						}
					}
					if len(error_t) == 0 {
						result = "OK"
					}
				}
//				fmt.Printf("done.\r\n")
				p_sdb.CloseData()
			}
		}	

		jr := JobResult{}
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
		var jd JobDelete 
		err = json.Unmarshal([]byte(body), &jd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := "Error"
		error_t := ""
		// every time initialize ??? 
		sdb := Init_SmallDB(jd.DB_path)
		sdb.Debug = 0
		p_sdb := &sdb
		if sdb.Inited {
			res, err := p_sdb.OpenDB()
			if err != nil {
				error_t = fmt.Sprintf("Error %v\r\n", err)
			} else {
				if res < 0 {
					error_t = fmt.Sprintf("Error %v\r\n", res)
				} else {					
					_, err1 := p_sdb.Delete_record(jd.NumRec)
					if err1 != nil {
						error_t = fmt.Sprintf("Error %v\r\n", err1)
					}
					if len(error_t) == 0 {
						result = "OK"
					}
				}
//				fmt.Printf("done.\r\n")
				p_sdb.CloseData()
			}
		}	

		jr := JobResult{}
		jr.Type = "delete" // type -> create add find read delete
		jr.Result = result
		jr.Error = error_t
//		jr.Records = rrec
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
/*
		http.HandleFunc(prefix+"/c/key", H_send_key)
		http.HandleFunc(prefix+"/c/login", H_login_s)
		http.HandleFunc(prefix+"/c/register", H_register_s)
*/

		prt := fmt.Sprintf(":%d", port)
		http.ListenAndServe(prt, nil)
	}()
	return 1
}
