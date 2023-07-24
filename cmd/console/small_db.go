package main

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"

	"encoding/json"
	"flag"
	"io/ioutil"

	"github.com/wanderer69/SmallDB/internal/common"
	expr "github.com/wanderer69/SmallDB/internal/expr"
	db "github.com/wanderer69/SmallDB/internal/index"
)

func LoadJob(name string) (*common.Job, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		fmt.Print(err)
		return nil, err
	}
	var s common.Job
	err = json.Unmarshal(data, &s)
	if err != nil {
		fmt.Println("error:", err)
		return nil, err
	}
	return &s, nil
}

func SaveJob(name string, s *common.Job) error {
	data_1, err2 := json.MarshalIndent(&s, "", "  ")
	if err2 != nil {
		fmt.Println("error:", err2)
		return err2
	}

	_ = ioutil.WriteFile(name, data_1, 0644)
	return nil
}

func main() {
	common.InitUniqueValue()
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
		s := common.DBDesc{
			Fields: []common.Field{
				{Name: "context"},
				{Name: "fact"},
				{Name: "obj"},
				{Name: "rel"},
				{Name: "subj"}},
			Indexes: []common.Index{
				{Fields: []string{"context"}},
				{Fields: []string{"fact"}},
				{Fields: []string{"obj"}},
				{Fields: []string{"rel"}},
				{Fields: []string{"subj"}},
			},
		}
		j := common.Job{
			Type:     "create",
			DB_path:  "./db1",
			CreateDB: s,
			AddRec: []common.FieldValue{
				{Name: "context", Value: "c*"},
				{Name: "fact", Value: "f*"},
				{Name: "obj", Value: "o*"},
				{Name: "rel", Value: "r*"},
				{Name: "subj", Value: "s*"},
			},
			Find: []common.FieldValue{
				{Name: "context", Value: "c*"},
				{Name: "fact", Value: "*"},
				{Name: "obj", Value: "*"},
				{Name: "rel", Value: "*"},
				{Name: "subj", Value: "*"},
			},
		}
		SaveJob("test1.json", &j)
		return
	}

	var job *common.Job = nil
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

	sdb := db.InitSmallDB(db_path)
	sdb.Debug = job.Debug

	switch cmd {
	case "create":
		if !sdb.Inited {
			fl := []string{}
			for i := range job.CreateDB.Fields {
				fl = append(fl, job.CreateDB.Fields[i].Name)
			}

			fmt.Printf("CreateDB ...")
			err := sdb.CreateDB(fl, db_path)
			if err != nil {
				fmt.Printf("CreateDB error %v\r\n", err)
				return
			}
			fmt.Printf("done\r\n")

			for i := range job.CreateDB.Indexes {
				inx := job.CreateDB.Indexes[i].Fields
				sdb.CreateIndex(inx)
			}

			fmt.Printf("CreateDB end\r\n")
		}
		return
	case "write":
		fmt.Printf("Write\r\n")
		err := sdb.OpenDB()
		if err != nil {
			fmt.Printf("Error %v\r\n", err)
			return
		}
		fl := []string{}
		el := []*expr.Expr{}
		for i := range job.AddRec {
			fl = append(fl, job.AddRec[i].Name)
			expr := expr.Expression_parse(job.AddRec[i].Value)
			if expr == nil {
				return
			}
			el = append(el, expr)
		}
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
		lt := []common.DataValue{}
		num_rec := job.NumRec
		for i := 0; i < num_rec; i++ {
			val := []string{}
			fva := []common.FieldValue{}
			for j := range fl {
				fv := common.FieldValue{Name: fl[j]}
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
					v := name + common.UniqueValue(d_size)
					val = append(val, v)
					fv.Value = v
				case "RandomStep":
					v, ok := mmd[fl[j]]
					if ok {
						if v.Value == 0 {
							p := el[j].Step
							d := common.UniqueValue(d_size)
							mmd[fl[j]] = IntCache{d, p}
							v.Value = p
							v.Key = d
						} else {
							v.Value = v.Value - 1
							mmd[fl[j]] = v
						}
					} else {
						p := el[j].Step
						d := common.UniqueValue(d_size)
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
			lt = append(lt, common.DataValue{Value: fva})
			if true {
				res, num, err := sdb.StoreRecordStrings(val)
				if err != nil {
					fmt.Printf("Error %v res %v num %v\r\n", err, res, num)
					return
				}
			}
		}
		fmt.Printf("done.\r\n")
		sdb.CloseData()

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
	case "read":
		sdb.OpenDB()
		num_rec := int64(job.NumRec)
		rec, res, err := sdb.LoadRecords(int(num_rec))
		if err != nil {
			fmt.Printf("res %v res %v\r\n", res, err)
		}
		fmt.Printf("res %v\r\n", res)
		for i := range rec {
			fmt.Printf("%v\r\n", rec[i])
		}
		return
	case "delete":
		sdb.OpenDB()
		num_rec := job.NumRec
		res, err := sdb.DeleteRecord(int64(num_rec))
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("res %v\r\n", res)
		return
	case "find":
		sdb.OpenDB()

		lt := []common.DataValue{}
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
		for i := range lt {
			lti := lt[i]
			for j := range lti.Value {
				name := lti.Value[j].Name
				val := lti.Value[j].Value
				fmt.Printf("name %v val %v\r\n", name, val)
				ds, err, err1 := sdb.FindRecordIndexString([]string{name}, []string{val})
				if err1 != nil {
					fmt.Printf("Error %v %v\r\n", err, err1)
					return
				}
				fmt.Printf("-> err %v\r\n", err)
				for k := range ds {
					fmt.Printf("ds[%v] %v\r\n", k, ds[k])
				}
			}
		}
		return
	}
}
