package small_db

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/wanderer69/SmallDB/internal/common"
	expr "github.com/wanderer69/SmallDB/internal/expr"
)

const pathToDB = "./testDB"

type SmallDBSuite struct {
	suite.Suite
}

func TestSmallDB(t *testing.T) {
	suite.Run(t, &SmallDBSuite{})
}

func (suite *SmallDBSuite) SetupTest() {
	err := os.RemoveAll(pathToDB)
	if err != nil {
		suite.T().Log("failed remove ", err)
	}
}

func (suite *SmallDBSuite) TearDownTest() {
	err := os.RemoveAll(pathToDB)
	if err != nil {
		suite.T().Log("failed remove ", err)
	}
}

func (suite *SmallDBSuite) TestInitSmallDB() {
	existsSDB := &SmallDB{
		Config: &SmallDBConfig{
			DataFileName:             "data.bin",
			BlocksFileName:           "blocks.bin",
			DeletedDataFileName:      "deleted.bin",
			DeletedDataIndexFileName: "deleted_inx.bin",
			JournalFileName:          "journal.bin",
			RowIndexFileName:         "row_index.bin",
			BlockSize:                22,
			HashTableSize:            HashTabSize,
			DatabaseName:             "database",
			IndexesMap:               make(map[string]IndexConfig),
		},
		Path:          pathToDB,
		FieldsNameMap: make(map[string]int),
	}
	suite.T().Run("no dir", func(t *testing.T) {
		sdb := InitSmallDB(pathToDB)
		sdb.Debug = 0
		_, err := os.Stat(pathToDB)
		suite.True(os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/config.json")
		suite.True(os.IsNotExist(err))
		suite.NotNil(sdb)
		suite.Equal(existsSDB, sdb)
	})
	suite.T().Run("directory exists", func(t *testing.T) {
		os.Mkdir(pathToDB, 0755)
		sdb := InitSmallDB(pathToDB)
		sdb.Debug = 0
		_, err := os.Stat(pathToDB)
		suite.True(!os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/config.json")
		suite.True(os.IsNotExist(err))
		suite.NotNil(sdb)
		suite.Equal(existsSDB, sdb)
	})
	suite.T().Run("directory exists, config.json exists", func(t *testing.T) {
		os.Mkdir(pathToDB, 0755)
		createEmptyFile := func(name string) {
			d := []byte("")
			suite.NoError(os.WriteFile(name, d, 0644))
		}
		createEmptyFile(pathToDB + "/config.json")
		sdb := InitSmallDB(pathToDB)
		sdb.Debug = 0
		_, err := os.Stat(pathToDB)
		suite.True(!os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/config.json")
		suite.True(!os.IsNotExist(err))
		suite.NotNil(sdb)
		suite.Equal(existsSDB, sdb)
	})
}

func (suite *SmallDBSuite) TestCreateDBOpenDBWriteDBReadDBFindDB() {
	existsSDB := &SmallDB{
		Config: &SmallDBConfig{
			DataFileName:             "data.bin",
			BlocksFileName:           "blocks.bin",
			DeletedDataFileName:      "deleted.bin",
			DeletedDataIndexFileName: "deleted_inx.bin",
			JournalFileName:          "journal.bin",
			RowIndexFileName:         "row_index.bin",
			BlockSize:                22,
			HashTableSize:            HashTabSize,
			DatabaseName:             "database",
			IndexesMap:               make(map[string]IndexConfig),
		},
		Path:          pathToDB,
		FieldsNameMap: make(map[string]int),
	}
	job := common.Job{
		NumRec: 5,
		CreateDB: common.DBDesc{
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
		},
		AddRec: []common.FieldValue{
			{Name: "context", Value: "=4*"},
			{Name: "fact", Value: "=2*"},
			{Name: "obj", Value: "=*"},
			{Name: "rel", Value: "=*"},
			{Name: "subj", Value: "=*"},
		},
		Find: []common.FieldValue{
			{Name: "context", Value: "c*"},
			{Name: "fact", Value: "*"},
			{Name: "obj", Value: "*"},
			{Name: "rel", Value: "*"},
			{Name: "subj", Value: "*"},
		},
	}

	suite.T().Run("no dir, CreateDB, CreateIndex", func(t *testing.T) {
		sdb := InitSmallDB(pathToDB)
		sdb.Debug = 0
		_, err := os.Stat(pathToDB)
		suite.True(os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/config.json")
		suite.True(os.IsNotExist(err))
		suite.NotNil(sdb)
		suite.False(sdb.Inited)
		suite.Equal(existsSDB, sdb)
		fl := []string{}
		for i := range job.CreateDB.Fields {
			fl = append(fl, job.CreateDB.Fields[i].Name)
		}
		err = sdb.CreateDB(fl, pathToDB)
		suite.NoError(err)

		for i := range job.CreateDB.Indexes {
			inx := job.CreateDB.Indexes[i].Fields
			suite.NoError(sdb.CreateIndex(inx))
		}
		_, err = os.Stat(pathToDB)
		suite.True(!os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/config.json")
		suite.True(!os.IsNotExist(err))

		_, err = os.Stat(pathToDB + "/" + sdb.Config.DataFileName)
		suite.True(!os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/" + sdb.Config.BlocksFileName)
		suite.True(!os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/" + sdb.Config.DeletedDataFileName)
		suite.True(os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/" + sdb.Config.DeletedDataIndexFileName)
		suite.True(os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/" + sdb.Config.JournalFileName)
		suite.True(os.IsNotExist(err))
		_, err = os.Stat(pathToDB + "/" + sdb.Config.RowIndexFileName)
		suite.True(!os.IsNotExist(err))
	})

	lt := []common.DataValue{}
	suite.T().Run("Write data", func(t *testing.T) {
		sdb := InitSmallDB(pathToDB)
		sdb.Debug = 0
		_, err := os.Stat(pathToDB)
		suite.True(!os.IsNotExist(err))
		suite.NotNil(sdb)
		suite.True(sdb.Inited)

		suite.NoError(sdb.OpenDB())
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
		fmt.Printf("Write begin...\r\n")
		numRec := job.NumRec
		expectedPos := []int{20, 166, 312, 458, 604}
		for i := 0; i < numRec; i++ {
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
				}
				fva = append(fva, fv)
			}
			lt = append(lt, common.DataValue{Value: fva})
			res, num, err := sdb.StoreRecordStrings(val)
			suite.NoError(err)
			suite.Equal(res, int64(expectedPos[i]))
			suite.Equal(num, int64(i))
		}
		sdb.CloseData()
		fmt.Printf("end\r\n")
	})
	suite.T().Run("Read data", func(t *testing.T) {
		sdb := InitSmallDB(pathToDB)
		sdb.Debug = 0
		_, err := os.Stat(pathToDB)
		suite.True(!os.IsNotExist(err))
		suite.NotNil(sdb)
		suite.True(sdb.Inited)

		suite.NoError(sdb.OpenDB())
		numRec := int64(job.NumRec)
		rec, num, err := sdb.LoadRecords(int(numRec))
		suite.NoError(err)
		suite.Equal(0, num)
		suite.Equal(int64(len(rec)), numRec)
		suite.Equal(len(lt), len(rec))
		for i := range rec {
			//fmt.Printf("%v %v\r\n", *rec[i], lt[i])
			for j := range rec[i].FieldsValue {
				val := lt[i].Value[j].Value
				suite.Equal(rec[i].FieldsValue[j], val)
			}
		}
	})
	suite.T().Run("Find data", func(t *testing.T) {
		sdb := InitSmallDB(pathToDB)
		sdb.Debug = 0
		_, err := os.Stat(pathToDB)
		suite.True(!os.IsNotExist(err))
		suite.NotNil(sdb)
		suite.True(sdb.Inited)

		suite.NoError(sdb.OpenDB())
		//numRec := int64(job.NumRec)
		expectedCounts := [][]int{{5, 3, 1, 1, 1}, {5, 3, 1, 1, 1}, {5, 3, 1, 1, 1}, {5, 2, 1, 1, 1}, {5, 2, 1, 1, 1}}

		for i := range lt {
			lti := lt[i]
			fmt.Printf("lt[%v] %v\r\n", i, lti.Value)
			for j := range lti.Value {
				name := lti.Value[j].Name
				val := lti.Value[j].Value
				fmt.Printf("name %v val %v\r\n", name, val)
				ds, err1, err := sdb.FindRecordIndexString([]string{name}, []string{val})
				suite.NoError(err)
				suite.Equal(0, err1)
				suite.Equal(expectedCounts[i][j], len(ds))
				for k := range ds {
					fmt.Printf("ds[%v] %v lti %v\r\n", k, ds[k], lti.Value)
					flag := true
					for m := range lt {
						flag = true
						for n := range lt[m].Value {
							val := lt[m].Value[n].Value
							if ds[k].FieldsValue[n] != val {
								flag = false
								break
							}
						}
						if flag {
							break
						}
					}
					suite.True(flag)
				}
			}
		}
	})
}
