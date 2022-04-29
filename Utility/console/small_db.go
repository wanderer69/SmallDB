package main

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"

	//	"strconv"
	"encoding/json"
	"flag"
	"io/ioutil"
	"time"

	. "arkhangelskiy-dv.ru/SmallDB/Expr"
	. "arkhangelskiy-dv.ru/SmallDB/v3"
	. "arkhangelskiy-dv.ru/SmallDB/common"
)

func Init_Unique_Value() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func Unique_Value(len_n int) string {
	var bytes_array []byte

	for i := 0; i < len_n; i++ {
		bytes := rand.Intn(35)
		if bytes > 9 {
			bytes = bytes + 7
		}
		bytes_array = append(bytes_array, byte(bytes+16*3))
	}
	str := string(bytes_array)
	return str
}

func LoadJob(name string) (*Job, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		fmt.Print(err)
		return nil, err
	}
	var s Job
	err = json.Unmarshal(data, &s)
	if err != nil {
		fmt.Println("error:", err)
		return nil, err
	}
	return &s, nil
}

func SaveJob(name string, s *Job) error {
	data_1, err2 := json.MarshalIndent(&s, "", "  ")
	if err2 != nil {
		fmt.Println("error:", err2)
		return err2
	}

	_ = ioutil.WriteFile(name, data_1, 0644)
	return nil
}

func main() {
	Init_Unique_Value()
	argsWithoutProg := os.Args[1:]
	if len(argsWithoutProg) == 0 {
		fmt.Printf("_ <base> <test>\r\n")
		return
	}
	BasePtr := flag.String("base", "", "name test base")
	TestPtr := flag.String("test", "", "test mode: create_index, create_index_free, write, read")
	FileName := flag.String("filename", "", "file name for create, add and find")
	flag.Parse()

	if false {
		s := DBDesc{
			Fields: []Field{
				Field{"context"},
				Field{"fact"},
				Field{"obj"},
				Field{"rel"},
				Field{"subj"}},
			Indexes: []Index{
				Index{[]string{"context"}},
				Index{[]string{"fact"}},
				Index{[]string{"obj"}},
				Index{[]string{"rel"}},
				Index{[]string{"subj"}},
			},
		}
		j := Job{
			Type:     "create",
			DB_path:  "./db1",
			CreateDB: s,
			AddRec: []FieldValue{
				FieldValue{"context", "c*"},
				FieldValue{"fact", "f*"},
				FieldValue{"obj", "o*"},
				FieldValue{"rel", "r*"},
				FieldValue{"subj", "s*"},
			},
			Find: []FieldValue{
				FieldValue{"context", "c*"},
				FieldValue{"fact", "*"},
				FieldValue{"obj", "*"},
				FieldValue{"rel", "*"},
				FieldValue{"subj", "*"},
			},
		}
		SaveJob("test1.json", &j)
		// LoadDBDesc
		return
	}

	type Local_trinar struct {
		Context string
		Fact    string
		Obj     string
		Rel     string
		Subj    string
	}
	var job *Job = nil
	fmt.Printf("fn %v\r\n", *FileName)
	if len(*FileName) > 0 {
		j, err := LoadJob(*FileName)
		if err != nil {
			fmt.Printf("Error %v\r\n", err)
			return
		}
		job = j
	}

	cmd := ""
	db_path := ""
	if len(*BasePtr) > 0 {
		if len(*TestPtr) > 0 {
			cmd = *TestPtr
			db_path = *BasePtr
		}
	} else {
		fmt.Printf("%v\r\n", job)
		if job != nil {
			cmd = job.Type
			db_path = job.DB_path
		} else {
			return
		}
	}
	fmt.Printf("cmd %v db_path %v\r\n", cmd, db_path)

	sdb := Init_SmallDB(db_path)
	sdb.Debug = job.Debug
	p_sdb := &sdb

	switch cmd {
	case "create":
		if !sdb.Inited {
			fl := []string{}
			for i, _ := range job.CreateDB.Fields {
				fl = append(fl, job.CreateDB.Fields[i].Name)
			}

			fmt.Printf("CreateDB ...")
			res := p_sdb.CreateDB(fl, db_path)
			if res < 0 {
				fmt.Printf("CreateDB error %v\r\n", res)
				return
			}
			fmt.Printf("done\r\n")

			for i, _ := range job.CreateDB.Indexes {
				inx := job.CreateDB.Indexes[i].Fields
				p_sdb.CreateIndex(inx)
			}

			// p_sdb.CreateIndexFree("index")
			fmt.Printf("CreateDB end\r\n")
		}
		return
	case "write":
		fmt.Printf("Write\r\n")
		res, err := p_sdb.OpenDB()
		if err != nil {
			fmt.Printf("Error %v\r\n", err)
			return
		}
		if res < 0 {
			fmt.Printf("Error %v\r\n", res)
		}
		fl := []string{}
		el := []*Expr{}
		for i, _ := range job.AddRec {
			fl = append(fl, job.AddRec[i].Name)
			expr := Expression_parse(job.AddRec[i].Value)
			//fmt.Printf("expr %v\r\n", expr)
			if expr == nil {
				return
			}
			el = append(el, expr)
			//fl = append(el, job.AddRec[i].Name)
		}
		// fmt.Printf("p_sdb %v\r\n", p_sdb)
		type IntCache struct {
			Key   string
			Value int
		}
		mmd := make(map[string]IntCache)
		rand.Seed(1000)
		var data_a []map[string]string
		if len(job.DataFile) > 0 {
				file, err := ioutil.ReadFile(job.DataFile)
				if err != nil {
					fmt.Printf("Error - %v\r\n", err)
				} else {
					_ = json.Unmarshal([]byte(file), &data_a)
				}
		}
		fmt.Printf("Write begin...\r\n")
		lt := []DataValue{}
		num_rec := job.NumRec
		for i := 0; i < num_rec; i++ {
			val := []string{}
			fva := []FieldValue{}
			for j, _ := range fl {
				// fmt.Printf("el[%v].Mode %v\r\n", j, el[j].Mode)
				fv := FieldValue{Name: fl[j]}
				d_size := 7
				if el[j].Len > 0 {
					d_size = el[j].Len
				}
				name := fl[j]
				if len(el[j].Symbol) > 0 {
					name = el[j].Symbol
				}
				switch el[j].Mode {
				case "Random":
					v := name + Unique_Value(d_size)
					val = append(val, v)
					fv.Value = v
				case "RandomStep":
					v, ok := mmd[fl[j]]
					if ok {
						if v.Value == 0 {
							p := el[j].Step
							d := Unique_Value(d_size)
							mmd[fl[j]] = IntCache{d, p}
							v.Value = p
							v.Key = d
						} else {
							v.Value = v.Value - 1
							mmd[fl[j]] = v
						}
					} else {
						p := el[j].Step
						d := Unique_Value(d_size)
						mmd[fl[j]] = IntCache{d, p}
						v.Value = p
						v.Key = d
					}
					vn := name + v.Key
					val = append(val, vn)
					fv.Value = vn
				case "ValueInt":
					v := name + fmt.Sprintf("%v", rand.Intn(el[j].ValueI))
					val = append(val, v)
					fv.Value = v
				case "FromFile":
					if i < len(data_a) {
						rec := data_a[i]
						v, ok := rec[name]
						if !ok {
							fmt.Printf("Error! Field %v not finded in data file\r\n", name)
							return
						} 
						val = append(val, v)
						fv.Value = v
					} else {
						fmt.Printf("Error! Field %v not have record num %v/ In file have only %v records\r\n", name, i, len(data_a))
						return
					}
				}
				fva = append(fva, fv)
			}
			lt = append(lt, DataValue{fva})
			// fmt.Printf("val %v\r\n", val)
			if true {
				res, num, err := p_sdb.Store_record_strings(val)
				if err != nil {
					fmt.Printf("Error %v res %v num %v\r\n", err, res, num)
					return
				}
			}
			// p_sdb.Store_record("один"+Unique_Value(7), "два"+Unique_Value(7), "три"+Unique_Value(7), "четыре"+Unique_Value(7), "пять"+Unique_Value(7))
		}
		fmt.Printf("done.\r\n")
		p_sdb.CloseData()

		dataFile, err1 := os.Create("./test_1")
		if err1 != nil {
			fmt.Println(err1)
			os.Exit(1)
		}

		// serialize the data
		dataEncoder := gob.NewEncoder(dataFile)
		dataEncoder.Encode(lt)

		dataFile.Close()
		fmt.Printf("File stored\r\n")
		return
		/*
			case "write1_1":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)
				for i := 0; i < 2; i++ {
					word_data := fmt.Sprintf("word%v", i)
					// data_data := fmt.Sprintf("word%v", i)
					for j := 0; j < 2; j++ {
						word_form_data := fmt.Sprintf("word_form_%v_%v", i, j)
						p_sdb.Store_record(word_form_data, word_data, "data"+Unique_Value(7))
						// fmt.Printf("word_form_data %v word_data %v\r\n", word_form_data, word_data)
					}
					p_sdb.Store_record("data"+Unique_Value(7), word_data, "data"+Unique_Value(7))
				}
				p_sdb.CloseData()
				return
			case "write1_2":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)
				for i := 0; i < 2; i++ {
					word_data := fmt.Sprintf("word%v", i)
					// data_data := fmt.Sprintf("word%v", i)
					for j := 0; j < 2; j++ {
						word_form_data := fmt.Sprintf("word_form_%v_%v", i, j)
						p_sdb.Store_record(word_form_data, word_data, " ")
						// fmt.Printf("word_form_data %v word_data %v\r\n", word_form_data, word_data)
					}
					p_sdb.Store_record(" ", word_data, "data"+Unique_Value(7))
				}
				p_sdb.CloseData()
				return
			case "write_free1":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)
				for i := 0; i < 1000; i++ {
					index_data := fmt.Sprintf("index_%v", i)
					one_data := fmt.Sprintf("один%v", i)
					pos_g, num, err := p_sdb.Store_record(one_data, "два"+Unique_Value(7), "три"+Unique_Value(7), "четыре"+Unique_Value(7), "пять"+Unique_Value(7))
					if err != nil {
						fmt.Printf("Error %v\r\n", err)
						return
					}
					fmt.Printf("pos_g %v, num %v\r\n", pos_g, num)
					p_sdb.StoreFreeIndex("index", index_data, pos_g)
				}
				p_sdb.CloseData()
				return
			case "write_free2":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)
				for i := 0; i < 1000; i++ {
					index_data := fmt.Sprintf("index_%v", i)
					one_data := fmt.Sprintf("слово%v", i)
					pos_g, num, err := p_sdb.Store_record(one_data, "два"+Unique_Value(7), "три"+Unique_Value(7))
					if err != nil {
						fmt.Printf("Error %v\r\n", err)
						return
					}
					fmt.Printf("pos_g %v, num %v\r\n", pos_g, num)
					p_sdb.StoreFreeIndex("word_index", index_data, pos_g)
				}
				p_sdb.CloseData()
				return
			case "write2":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)
				// первичное формирование данных
				context_cnt := 0
				fact_cnt := 0
				lt := []Local_trinar{}
				for i := 0; i < 1000; i++ {
					obj := "объект" + Unique_Value(7)
					rel := "отношение" + Unique_Value(7)
					subj := "субъект" + Unique_Value(7)
					context := "контекст" + fmt.Sprintf("_%0d", context_cnt)
					fact := "факт" + fmt.Sprintf("_%0d", fact_cnt)
					p_sdb.Store_record(context, fact, obj, rel, subj)
					context_cnt = context_cnt + 1
					if context_cnt > 99 {
						context_cnt = 0
					}
					fact_cnt = fact_cnt + 1
					if fact_cnt > 9 {
						fact_cnt = 0
					}
					lti := Local_trinar{}
					lti.Context = context
					lti.Fact = fact
					lti.Obj = obj
					lti.Rel = rel
					lti.Subj = subj
					lt = append(lt, lti)
				}
				p_sdb.CloseData()

				dataFile, err1 := os.Create("./test_1")
				if err1 != nil {
					fmt.Println(err1)
					os.Exit(1)
				}

				// serialize the data
				dataEncoder := gob.NewEncoder(dataFile)
				dataEncoder.Encode(lt)

				dataFile.Close()
				fmt.Printf("Stored file\r\n")
				return
			case "write3":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)

				// повторное формирование из сохраненных данных
				var data []Local_trinar

				// open data file
				dataFile, err := os.Open("./test_1")

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

				context_cnt := 0
				fact_cnt := 0
				flag := false
				for i := 0; i < 1000; i++ {
					fmt.Printf("data[i] %v\r\n", data[i])
					obj := data[i].Obj
					rel := data[i].Rel
					subj := data[i].Subj
					context := data[i].Context
					fact := data[i].Fact
					p_sdb.Store_record(context, fact, obj, rel, subj)
					context_cnt = context_cnt + 1
					if context_cnt > 99 {
						context_cnt = 0
					}
					fact_cnt = fact_cnt + 1
					if fact_cnt > 9 {
						fact_cnt = 0
					}
					for j, ihs := range sdb.IhsA {
						mask := ihs.Mask
						find_args := create_find_args((int)(p_sdb.Dhs.Field_qty), mask, data[i])
						ds, err, err1 := p_sdb.Find_record(j, find_args...)
						if err1 != nil {
							fmt.Printf("Error %v %v\r\n", err, err1)
							return
						}
						if len(ds) == 0 {
							fmt.Printf("i %v, j %v, mask %v, find_args %v, ds %v, err %v\r\n", i, j, mask, find_args, ds, err)
							flag = true
							break
						}
					}
					if flag {
						break
					}
				}
				p_sdb.CloseData()
				return
			case "write4":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)

				// повторное формирование из сохраненных данных
				var data []Local_trinar

				// open data file
				dataFile, err := os.Open("./test_1")

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

				context_cnt := 0
				fact_cnt := 0
				// flag := false
				for i := 0; i < 1000; i++ {
					// fmt.Printf("data[i] %v\r\n", data[i])
					obj := data[i].Obj
					rel := data[i].Rel
					subj := data[i].Subj
					context := data[i].Context
					fact := data[i].Fact
					p_sdb.Store_record(context, fact, obj, rel, subj)
					context_cnt = context_cnt + 1
					if context_cnt > 99 {
						context_cnt = 0
					}
					fact_cnt = fact_cnt + 1
					if fact_cnt > 9 {
						fact_cnt = 0
					}
				}
				p_sdb.CloseData()
				return
			case "write5":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)
				a1 := "один" + Unique_Value(7)
				a2 := "два" + Unique_Value(7)
				a3 := "три" + Unique_Value(7)
				a4 := "четыре" + Unique_Value(7)
				a5 := "пять" + Unique_Value(7)
				p_sdb.Store_record(a1, a2, a3, a4, a5)
				a2 = "два" + Unique_Value(7)
				a3 = "три" + Unique_Value(7)
				a4 = "четыре" + Unique_Value(7)
				a5 = "пять" + Unique_Value(7)
				p_sdb.Store_record(a1, a2, a3, a4, a5)
				ds, err, err1 := p_sdb.Find_record(0, a1)
				if err1 != nil {
					fmt.Printf("Error %v %v\r\n", err, err1)
					return
				}
				fmt.Printf("ds %v err %v\r\n", ds, err)
				p_sdb.CloseData()
				return
			case "write6":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)
				a1 := "композитами"
				a2 := "два" + Unique_Value(7)
				a3 := "три" + Unique_Value(7)
				a4 := "четыре" + Unique_Value(7)
				a5 := "пять" + Unique_Value(7)
				p_sdb.Store_record(a1, a2, a3, a4, a5)
				a2 = "два" + Unique_Value(7)
				a3 = "три" + Unique_Value(7)
				a4 = "четыре" + Unique_Value(7)
				a5 = "пять" + Unique_Value(7)
				p_sdb.Store_record(a1, a2, a3, a4, a5)
				ds, err, err1 := p_sdb.Find_record(0, a1)
				if err1 != nil {
					fmt.Printf("Error %v %v\r\n", err, err1)
					return
				}
				fmt.Printf("ds %v err %v\r\n", ds, err)
				p_sdb.CloseData()
				return
			case "write7":
				p_sdb.OpenDB()
				// fmt.Printf("p_sdb %v\r\n", p_sdb)
				a1 := "композитами"
				for i := 0; i < 28; i++ {
					a2 := "два" + Unique_Value(7)
					a3 := "три" + Unique_Value(7)
					a4 := "четыре" + Unique_Value(7)
					a5 := "пять" + Unique_Value(7)
					p_sdb.Store_record(a1, a2, a3, a4, a5)

					ds, err, err1 := p_sdb.Find_record(0, a1)
					if err1 != nil {
						fmt.Printf("Error %v %v\r\n", err, err1)
						return
					}
					fmt.Printf("i %v len %v ds %v err %v\r\n", i, len(ds), ds, err)
				}

				//	a2 = "два"+Unique_Value(7)
				//	a3 = "три"+Unique_Value(7)
				//	a4 = "четыре"+Unique_Value(7)
				//	a5 = "пять"+Unique_Value(7)
				//	p_sdb.Store_record(a1, a2, a3, a4, a5)

				p_sdb.CloseData()
				return
		*/
	case "read":
		p_sdb.OpenDB()
		// fmt.Printf("p_sdb %v\r\n", p_sdb)
		num_rec := int64(job.NumRec)
		rec, res, err := p_sdb.Load_record(num_rec)
		if err != nil {
			fmt.Printf("res %v res %v\r\n", res, err)
			// return
		}
		fmt.Printf("res %v\r\n", res)
		for i, _ := range rec {
			fmt.Printf("%v\r\n", rec[i])
		}
		return
	case "delete":
		p_sdb.OpenDB()
		// p_sdb.Debug = 10
		// fmt.Printf("p_sdb %v\r\n", p_sdb)
		num_rec := job.NumRec
		res, err := p_sdb.Delete_record(num_rec)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("res %v\r\n", res)
/*
		for i, _ := range rec {
			fmt.Printf("%v\r\n", rec[i])
		}
*/
		return
	case "find":
		p_sdb.OpenDB()

		lt := []DataValue{}
		// open data file
		dataFile, err := os.Open("./test_1")

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		dataDecoder := gob.NewDecoder(dataFile)
		err = dataDecoder.Decode(&lt)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		dataFile.Close()
		// fmt.Printf("p_sdb %v\r\n", p_sdb)
		for i, _ := range lt {
			lti := lt[i]
			for j, _ := range lti.Value {
				name := lti.Value[j].Name
				val := lti.Value[j].Value
				fmt.Printf("name %v val %v\r\n", name, val)
				ds, err, err1 := p_sdb.Find_record_index_string(name, []string{val})
				if err1 != nil {
					fmt.Printf("Error %v %v\r\n", err, err1)
					return
				}
				fmt.Printf("-> ds %v err %v\r\n", ds, err)
			}
		}
		return
/*
	case "find1_1":
		p_sdb.OpenDB()
		// fmt.Printf("p_sdb %v\r\n", p_sdb)
		for i := 0; i < 2; i++ {
			// word_data := fmt.Sprintf("word%v", i)
			for j := 0; j < 2; j++ {
				//word_form_data := fmt.Sprintf("word_form_%v", j)
				word_form_data := fmt.Sprintf("word_form_%v_%v", i, j)
				wfdl := []string{word_form_data}
				ds, err, err1 := p_sdb.Find_record(0, wfdl...)
				if err1 != nil {
					fmt.Printf("Error %v %v\r\n", err, err1)
					return
				}
				fmt.Printf("ds %v err %v\r\n", ds, err)
				if err == 0 {
					if len(ds) > 0 {
						res := ds[0]
						if len(res) > 0 {
							word_data1 := res[1]
							ds1, err1, err2 := p_sdb.Find_record(1, word_data1)
							if err2 != nil {
								fmt.Printf("Error %v %v\r\n", err1, err2)
								return
							}
							fmt.Printf("ds1 %v err1 %v\r\n", ds1, err1)
						}
					}
				}
			}
		}
		return

	case "find_index1":
		p_sdb.OpenDB()
		// fmt.Printf("p_sdb %v\r\n", p_sdb)
		index_id := p_sdb.GetFreeIndexId("index")
		for i := 0; i < 1000; i++ {
			index_data := fmt.Sprintf("index_%v", i)
			//one_data := fmt.Sprintf("один%v", i)
			ds, err, err1 := p_sdb.Find_record(int(index_id), index_data)
			if err1 != nil {
				fmt.Printf("Error %v %v\r\n", err, err1)
				return
			}
			fmt.Printf("ds %+v err %v\r\n", ds, err)
			//p_sdb.Store_recordFreeIndex("index", index_data, one_data, "два"+Unique_Value(7), "три"+Unique_Value(7), "четыре"+Unique_Value(7), "пять"+Unique_Value(7))
		}
		p_sdb.CloseData()
		return

	case "find_index2":
		p_sdb.OpenDB()
		// fmt.Printf("p_sdb %v\r\n", p_sdb)
		index_id := p_sdb.GetFreeIndexId("word_index")
		for i := 0; i < 1000; i++ {
			index_data := fmt.Sprintf("index_%v", i)
			//one_data := fmt.Sprintf("один%v", i)
			ds, err, err1 := p_sdb.Find_record(int(index_id), index_data)
			if err1 != nil {
				fmt.Printf("Error %v %v\r\n", err, err1)
				return
			}
			fmt.Printf("ds %+v err %v\r\n", ds, err)
			//p_sdb.Store_recordFreeIndex("index", index_data, one_data, "два"+Unique_Value(7), "три"+Unique_Value(7), "четыре"+Unique_Value(7), "пять"+Unique_Value(7))
		}
		p_sdb.CloseData()
		return

	case "find2":
		p_sdb.OpenDB()
		// fmt.Printf("p_sdb %v\r\n", p_sdb)
		// пробник для отдельных случаев
		ds, err, err1 := p_sdb.Find_record((int)(sdb.IhsA[1].Mask), "факт_0")
		if err1 != nil {
			fmt.Printf("Error %v %v\r\n", err, err1)
			return
		}
		fmt.Printf("ds %v err %v\r\n", ds, err)
		return

	case "find3":
		p_sdb.OpenDB()
		// fmt.Printf("p_sdb %v\r\n", p_sdb)
		var data []Local_trinar

		// open data file
		dataFile, err := os.Open("./test_1")

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

		local_debug := 0

		error_list := [][]string{}
		for st, lti := range data {
			//fmt.Printf("lti %#v\r\n", lti)
			// читаем запись и ищем по очереди записи
			for i, ihs := range sdb.IhsA {
				mask := ihs.Mask
				//00001
				find_args := create_find_args((int)(p_sdb.Dhs.Field_qty), mask, lti)
				//fmt.Printf("find_args %v i %v, ihs %+v\r\n", find_args, i, ihs)
				ds, err, err1 := p_sdb.Find_record(i, find_args...)
				if err1 != nil {
					fmt.Printf("Error %v %v\r\n", err, err1)
					return
				}
				if local_debug > 1 {
					fmt.Printf("ds %v err %v\r\n", ds, err)
				}
				err_l := []string{}
				if len(ds) > 0 {
					// flag_cond := true
					for _, dsi := range ds {
						err_s, res := compare_find_args((int)(p_sdb.Dhs.Field_qty), mask, lti, dsi)
						if res {
							err_l = append(err_l, err_s)
						}
					}
				} else {
					err_s := fmt.Sprintf("Data not found by stage %v, ind %v mask %v find_args %v", st, i, mask, find_args)
					err_l = append(err_l, err_s)
				}
				if len(err_l) > 0 {
					error_list = append(error_list, err_l)
				}
			}
		}
		for _, sl := range error_list {
			fmt.Printf("%v\r\n", sl)
		}
		return
*/
	}
}
