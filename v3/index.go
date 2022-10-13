package small_db

import (
	"fmt"
	"io/ioutil"

	"strings"

	"bytes"
	"encoding/json"

	"encoding/binary"
	"hash"
	"hash/fnv"
	"os"
	"errors"
	"reflect"

	"regexp"
	uuid "github.com/satori/go.uuid"
	. "github.com/wanderer69/SmallDB/common"	
)

const HASHTAB_SIZE = 0x3FFFF
const HASHTAB_MUL = 31

type Index_config struct {
	FieldsName []string `json:"fields_name"`
	Free       bool
	Mask       int64
}

type SmallDB_config struct {
	Data_file_name              string                  `json:"data_file_name"`
	Index_files_name            []string                `json:"index_files_name"`
	FreeIndex_files_name        []string                `json:"free_index_files_name"`
	Blocks_file_name            string                  `json:"blocks_file_name"`
	DeletedData_file_name       string                  `json:"deleted_data_file_name"`
	DeletedData_index_file_name string                  `json:"deleted_data_index_file_name"`
	Journal_file_name           string                  `json:"journal_file_name"`
	RowIndex_file_name          string                  `json:"row_index_file_name"`
	Block_size                  int32                   `json:"block_size"`
	HashTable_size              uint32                  `json:"hash_table_size"`
	UseSync                     int8                    `json:"use_sync"`
	UseJournal                  int8                    `json:"use_journal"`
	UseDeletedData              int8                    `json:"use_deleted_data"`
	DatabaseName                string                  `json:"database_name"`
	FieldsName                  []string                `json:"fields_name"`
	Indexes                     []Index_config          `json:"indexes"`
	IndexesMap                  map[string]Index_config `json:"indexes_map"`
}

type SmallDB struct {
	Config                 SmallDB_config
	Path                   string
	Inited                 bool // если false - то значит не создана
	Opened                 bool // если false - то значит не открыта
	RowIndex_file          *os.File
	Index_files            []*os.File
	Data_file              *os.File
	Block_file             *os.File
	DeletedData_file       *os.File
	DeletedData_index_file *os.File
	Journal_file           *os.File
	FreeIndexData_files    map[int]*os.File
	Debug                  int
	Dhs                    Data_header_struct
	DDhs                   Data_header_struct
	Bhs                    Block_header_struct
	IhsA                   []Index_header_struct
	RIIhs                  Index_header_struct
	DDIhs                  Index_header_struct
	FIDhsA                 map[int]FreeIndexData_header_struct
	FieldsNameMap          map[string]int
	Cnt                    int64
}

func Init_SmallDB(path string) SmallDB {
	sdb := SmallDB{}
	file, err := ioutil.ReadFile(path + "/" + "config.json")
	if err != nil {
		sdbc := SmallDB_config{}
		sdbc.Data_file_name = "data.bin"
		// индексов нет.
		// sdbc.Index_files_name = append(sdbc.Index_files_name, "index0.bin")
		sdbc.Blocks_file_name = "blocks.bin"
		sdbc.DeletedData_file_name = "deleted.bin"
		sdbc.DeletedData_index_file_name = "deleted_inx.bin"
		sdbc.Journal_file_name = "journal.bin"
		sdbc.RowIndex_file_name = "row_index.bin"
		sdbc.Block_size = 22
		sdbc.HashTable_size = HASHTAB_SIZE
		sdbc.DatabaseName = "database"
		sdbc.UseSync = 0
		sdbc.UseJournal = 0
		sdbc.UseDeletedData = 0
		sdbc.IndexesMap = make(map[string]Index_config)
		// fmt.Printf("sdbc %v\r\n", sdbc)
		sdb.Config = sdbc
		sdb.Inited = false
	} else {
		_ = json.Unmarshal([]byte(file), &sdb.Config)
		//sdb.Block
		sdb.Inited = true
	}
	sdb.FieldsNameMap = make(map[string]int)
	for i, _ := range sdb.Config.FieldsName {
		sdb.FieldsNameMap[sdb.Config.FieldsName[i]] = i
	}
	sdb.Path = path
	sdb.Data_file = nil
	sdb.Block_file = nil
	sdb.Debug = 0
	sdb.Cnt = 0
	sdb.Opened = false
	return sdb
}

func (sdb *SmallDB) Store_Config_SmallDB() error {
	ba, _ := json.MarshalIndent(sdb.Config, "", "  ")
	err := os.Chmod(sdb.Path+"/"+"config.json", 0777)
	if err != nil {
		fmt.Println(err)
		// return err
	}
	err1 := ioutil.WriteFile(sdb.Path+"/"+"config.json", ba, 0777)
	if err1 != nil {
		fmt.Println(err1)
		os.Exit(-1)
	}
	return nil
}

type Index_header_struct struct {
	Id     int64 // идентификатор индекса
	Mask   int64 // маска индекса (каждый бит это признак присутствия в индексе поля)
	IsFree uint16
}

const Index_header_structLen = 16 + 2

type Index_struct struct {
	Number      int64 // идентификатор индексной записи
	PointerFar  int64 // указатель на блок
	PointerNear int32 // указатель внутри блока
	State       int16 // признак состояния блока     (введено для исправления дефекта определения незанятого индекса)
}

const INDEX_USED = 1
const Index_structLen = 8 + 8 + 4 + 2

type Block_header_struct struct {
	Id               int64 // идентификатор индекса
	PointerNextBlock int64 // идентификатор на следующий блок
	PointerPrevBlock int64 // идентификатор на предыдущий блок
	Size             int32 // размер блока
}

const Block_header_structLen = 8 + 8 + 8 + 4

type Block_struct struct {
	Id          int64 // идентификатор индекса
	PointerData int64 // указатель на данные
	PointerFar  int64 // указатель на следующую запись дальний (на блок)
	PointerNear int32 // указатель на следующую запись внутри блока
}

const Block_structLen = 8 + 8 + 8 + 4

type Data_header_struct struct {
	Id        int64 // идентификатор данных
	Cnt       int64 // счетчик номера записи
	Field_qty int32 // количество полей в одной записи
}

const Data_header_structLen = 8 + 8 + 4

type Data_struct struct {
	Id      int64 // идентификатор записи
	State   int16 // статус записи 0 - сохранена 1 - требует удаления
	Field   int32 // номер поля -1 - RowID
	DataLen int32 // длина строки с данными
}

const Data_structLen = 8 + 2 + 4 + 4

type FreeIndexData_header_struct struct {
	Id  int64 // идентификатор индекса файла значений свободного индекса
	Cnt int64 // счетчик номера записи
}

const FreeIndexData_header_structLen = 8 + 8

type FreeIndexData_struct struct {
	Id      int64 // идентификатор записи
	State   int16 // статус записи 0 - сохранена 1 - требует удаления
	DataLen int32 // длина строки с данными
}

const FreeIndexData_structLen = 8 + 2 + 4 + 4

func From_Index_header(ihs Index_header_struct) ([]byte, int, error) {
	// длина
	len_header := Index_header_structLen
	var buf = bytes.NewBuffer(make([]byte, 0, len_header))
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &ihs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	return buf.Bytes(), len_header, nil
}

func To_Index_header(b_in []byte) (Index_header_struct, int, error) {
	var ihs Index_header_struct
	var buf = bytes.NewBuffer(make([]byte, 0, len(b_in)))
	if err := binary.Write(buf, binary.BigEndian, &b_in); err != nil {
		fmt.Println(err)
		return ihs, -1, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &ihs); err != nil {
		fmt.Println(err)
		return ihs, -2, err
	}
	// длина
	len_header := Index_header_structLen
	return ihs, len_header, nil
}

func From_Index(is Index_struct) ([]byte, int, error) {
	// длина
	len_header := Index_structLen
	var buf = bytes.NewBuffer(make([]byte, 0, len_header))
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &is); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	return buf.Bytes(), len_header, nil
}

func To_Index(b_in []byte) (Index_struct, int, error) {
	var is Index_struct
	var buf = bytes.NewBuffer(make([]byte, 0, len(b_in)))
	if err := binary.Write(buf, binary.BigEndian, &b_in); err != nil {
		fmt.Println(err)
		return is, -1, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &is); err != nil {
		fmt.Println(err)
		return is, -2, err
	}
	// длина
	len_header := Index_structLen
	return is, len_header, nil
}

func From_Block_header(bhs Block_header_struct) ([]byte, int, error) {
	// длина
	len_header := Block_header_structLen
	var buf = bytes.NewBuffer(make([]byte, 0, len_header))
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &bhs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), len_header, nil
}

func To_Block_header(b_in []byte) (Block_header_struct, int, error) {
	var bhs Block_header_struct
	var buf = bytes.NewBuffer(make([]byte, 0, len(b_in)))
	if err := binary.Write(buf, binary.BigEndian, &b_in); err != nil {
		fmt.Println(err)
		return bhs, -1, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &bhs); err != nil {
		fmt.Println(err)
		return bhs, -2, err
	}
	// длина
	len_header := Block_header_structLen
	return bhs, len_header, nil
}

func From_Block(bs Block_struct) ([]byte, int, error) {
	// длина
	len_header := Block_header_structLen
	var buf = bytes.NewBuffer(make([]byte, 0, len_header))
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &bs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	return buf.Bytes(), len_header, nil
}

func To_Block(b_in []byte) (Block_struct, int, error) {
	var bs Block_struct
	var buf = bytes.NewBuffer(make([]byte, 0, len(b_in)))
	if err := binary.Write(buf, binary.BigEndian, &b_in); err != nil {
		fmt.Println(err)
		return bs, -1, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &bs); err != nil {
		fmt.Println(err)
		return bs, -2, err
	}
	// длина
	len_header := Block_structLen
	return bs, len_header, nil
}

func From_Data_header(dhs Data_header_struct) ([]byte, int, error) {
	len_header := Data_header_structLen
	b_in := make([]byte, 0, len_header)
	var buf = bytes.NewBuffer(b_in)
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &dhs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), len_header, nil
}

func To_Data_header(b_in []byte) (Data_header_struct, int, error) {
	var dhs Data_header_struct
	var buf = bytes.NewBuffer(make([]byte, 0, len(b_in)))
	if err := binary.Write(buf, binary.BigEndian, &b_in); err != nil {
		fmt.Println(err)
		return dhs, -1, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &dhs); err != nil {
		fmt.Println(err)
		return dhs, -2, err
	}
	// длина
	len_header := Data_header_structLen
	return dhs, len_header, nil
}

func From_Data(ds Data_struct) ([]byte, int, error) {
	len_header := Data_structLen //(int)(unsafe.Sizeof(dhs))
	b_in := make([]byte, 0, len_header)
	var buf = bytes.NewBuffer(b_in)
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &ds); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), len_header, nil
}

func To_Data(b_in []byte) (Data_struct, int, error) {
	var ds Data_struct
	var buf = bytes.NewBuffer(make([]byte, 0, len(b_in)))
	if err := binary.Write(buf, binary.BigEndian, &b_in); err != nil {
		fmt.Println(err)
		return ds, -1, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &ds); err != nil {
		fmt.Println(err)
		return ds, -2, err
	}
	// длина
	len_header := Data_structLen
	return ds, len_header, nil
}

func From_FreeIndexData_header(dhs FreeIndexData_header_struct) ([]byte, int, error) {
	len_header := FreeIndexData_header_structLen
	b_in := make([]byte, 0, len_header)
	var buf = bytes.NewBuffer(b_in)
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &dhs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), len_header, nil
}

func To_FreeIndexData_header(b_in []byte) (FreeIndexData_header_struct, int, error) {
	var dhs FreeIndexData_header_struct
	var buf = bytes.NewBuffer(make([]byte, 0, len(b_in)))
	if err := binary.Write(buf, binary.BigEndian, &b_in); err != nil {
		fmt.Println(err)
		return dhs, -1, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &dhs); err != nil {
		fmt.Println(err)
		return dhs, -2, err
	}
	// длина
	len_header := FreeIndexData_header_structLen
	return dhs, len_header, nil
}

func From_FreeIndexData(ds FreeIndexData_struct) ([]byte, int, error) {
	len_header := FreeIndexData_structLen
	b_in := make([]byte, 0, len_header)
	var buf = bytes.NewBuffer(b_in)
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &ds); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), len_header, nil
}

func To_FreeIndexData(b_in []byte) (FreeIndexData_struct, int, error) {
	var ds FreeIndexData_struct
	var buf = bytes.NewBuffer(make([]byte, 0, len(b_in)))
	if err := binary.Write(buf, binary.BigEndian, &b_in); err != nil {
		fmt.Println(err)
		return ds, -1, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &ds); err != nil {
		fmt.Println(err)
		return ds, -2, err
	}
	// длина
	len_header := FreeIndexData_structLen
	return ds, len_header, nil
}

const ROW_INDEX_ID = 0xFFFF0000

// delete data index
func (sdb *SmallDB) OpenDeletedData_index() {
	// Open a new file for writing only
	file, err := os.OpenFile(
		sdb.Path+"/"+sdb.Config.DeletedData_index_file_name,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenDeletedData_index ")
		fmt.Println(err)
		os.Exit(-1)
	}
	sdb.DeletedData_index_file = file
}

func (sdb *SmallDB) CloseDeletedData_index() {
	sdb.DeletedData_index_file.Close()
}

func (sdb *SmallDB) WriteDeletedData_index(pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteDeletedData_index")
	}
	var pos_res int64 = 0
	if pos == -1 {
		newPosition, err := sdb.DeletedData_index_file.Seek(0, 2)
		if err != nil {
			fmt.Print("WriteDeletedData_index ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	} else {
		newPosition, err := sdb.DeletedData_index_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("WriteDeletedData_index ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.DeletedData_index_file.Write(byteSlice)
	if err != nil {
		fmt.Print("WriteDeletedData_index ")
		//log.Fatal(err)
		fmt.Println(err)
		os.Exit(-1)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return pos_res
}

func (sdb *SmallDB) ReadDeletedData_index(pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadDeletedData_index")
	}
	if sdb.Config.UseSync > 0 {
		sdb.Journal_file.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.DeletedData_index_file.Seek(0, 2)
		if err != nil {
			fmt.Print("ReadDeletedData_index ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := sdb.DeletedData_index_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("ReadDeletedData ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}
	byteSlice := make([]byte, len)
	bytesRead, err := sdb.DeletedData_index_file.Read(byteSlice)
	if err != nil {
		fmt.Print("ReadDeletedData_index ")
		fmt.Println(err)
		os.Exit(-1)
		// log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Number of bytes read: %d\n", bytesRead)
		fmt.Printf("Data read: %s\n", byteSlice)
	}
	return byteSlice
}

// delete data
func (sdb *SmallDB) OpenDeletedData() {
	// Open a new file for writing only
	file, err := os.OpenFile(
		sdb.Path+"/"+sdb.Config.DeletedData_file_name,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenDeletedData ")
		fmt.Println(err)
		os.Exit(-1)
		// log.Fatal(err)
	}
	sdb.DeletedData_file = file
}

func (sdb *SmallDB) CloseDeletedData() {
	sdb.DeletedData_file.Close()
}

func (sdb *SmallDB) WriteDeletedData(pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteDeletedData")
	}
	var pos_res int64 = 0
	if pos == -1 {
		newPosition, err := sdb.DeletedData_file.Seek(0, 2)
		if err != nil {
			fmt.Print("WriteDeletedData ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	} else {
		newPosition, err := sdb.DeletedData_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("WriteDeletedData ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.DeletedData_file.Write(byteSlice)
	if err != nil {
		fmt.Print("WriteDeletedData ")
		fmt.Println(err)
		os.Exit(-1)
		// log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return pos_res
}

func (sdb *SmallDB) ReadDeletedData(pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadDeletedData")
	}
	if sdb.Config.UseSync > 0 {
		sdb.Journal_file.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.DeletedData_file.Seek(0, 2)
		if err != nil {
			fmt.Print("ReadDeletedData ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := sdb.DeletedData_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("ReadDeletedData ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}
	byteSlice := make([]byte, len)
	bytesRead, err := sdb.DeletedData_file.Read(byteSlice)
	if err != nil {
		fmt.Print("ReadDeletedData ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Number of bytes read: %d\n", bytesRead)
		fmt.Printf("Data read: %s\n", byteSlice)
	}
	return byteSlice
}

// journal data
func (sdb *SmallDB) OpenJournal() {
	// Open a new file for writing only
	file, err := os.OpenFile(
		sdb.Path+"/"+sdb.Config.Journal_file_name,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenJournal ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	sdb.Journal_file = file
}

func (sdb *SmallDB) CloseJournal() {
	sdb.Journal_file.Close()
}

func (sdb *SmallDB) WriteJournal(pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteJournal")
	}
	var pos_res int64 = 0
	if pos == -1 {
		newPosition, err := sdb.Journal_file.Seek(0, 2)
		if err != nil {
			fmt.Print("WriteJournal ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	} else {
		newPosition, err := sdb.Journal_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("WriteJournal ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.Journal_file.Write(byteSlice)
	if err != nil {
		fmt.Print("WriteJournal ")
		fmt.Println(err)
		os.Exit(-1)
		// log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return pos_res
}

func (sdb *SmallDB) ReadJournal(pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadJournal")
	}
	if sdb.Config.UseSync > 0 {
		sdb.Journal_file.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.Journal_file.Seek(0, 2)
		if err != nil {
			fmt.Print("ReadJournal ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := sdb.Journal_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("ReadJournal ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}
	byteSlice := make([]byte, len)
	bytesRead, err := sdb.Journal_file.Read(byteSlice)
	if err != nil {
		fmt.Print("ReadJournal ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Number of bytes read: %d\n", bytesRead)
		fmt.Printf("Data read: %s\n", byteSlice)
	}
	return byteSlice
}

// index
func (sdb *SmallDB) OpenIndex(index_id int) int {
	// Open a new file for writing only
	f_name := ""
	if index_id == ROW_INDEX_ID {
		f_name = sdb.Path + "/" + sdb.Config.RowIndex_file_name
	} else {
		f_name = sdb.Path + "/" + sdb.Config.Index_files_name[index_id]
	}
	file, err := os.OpenFile(
		f_name,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenIndex ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if index_id == ROW_INDEX_ID {
		sdb.RowIndex_file = file
	} else {
		sdb.Index_files = append(sdb.Index_files, file)
	}
	return index_id
}

func (sdb *SmallDB) CloseIndex(index_id int) {
	if index_id == ROW_INDEX_ID {
		sdb.RowIndex_file.Close()
	} else {
		sdb.Index_files[index_id].Close()
	}
}

func (sdb *SmallDB) WriteIndex(index_id int, pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteIndex")
	}
	var f *os.File
	if index_id == ROW_INDEX_ID {
		f = sdb.RowIndex_file
	} else {
		f = sdb.Index_files[index_id]
	}
	var pos_res int64 = 0
	if pos == -1 {
		newPosition, err := f.Seek(0, 2)
		if err != nil {
			fmt.Print("WriteIndex ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	} else {
		newPosition, err := f.Seek(pos, 0)
		if err != nil {
			fmt.Print("WriteIndex ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := f.Write(byteSlice)
	if err != nil {
		fmt.Print("WriteIndex ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return pos_res
}

func (sdb *SmallDB) ReadIndex(index_id int, pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadIndex")
	}
	var f *os.File
	if index_id == ROW_INDEX_ID {
		f = sdb.RowIndex_file
	} else {
		f = sdb.Index_files[index_id]
	}
	if sdb.Config.UseSync > 0 {
		f.Sync()
	}
	if pos == -1 {
		newPosition, err := f.Seek(0, 2)
		if err != nil {
			fmt.Print("ReadIndex seek 2 error ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 1 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := f.Seek(pos, 0)
		if err != nil {
			fmt.Printf("ReadIndex seek %v 0 error ", pos)
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}
	byteSlice := make([]byte, len)
	bytesRead, err := f.Read(byteSlice)
	if err != nil {
		fmt.Print("ReadIndex read error ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Number of bytes read: %d\n", bytesRead)
		fmt.Printf("Data read: %s\n", byteSlice)
	}
	return byteSlice
}

// data
func (sdb *SmallDB) OpenData() {
	// Open a new file for writing only
	file, err := os.OpenFile(
		sdb.Path+"/"+sdb.Config.Data_file_name,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenData ")
		fmt.Println(err)
		os.Exit(-1)
		// log.Fatal(err)
	}
	sdb.Data_file = file
}

func (sdb *SmallDB) CloseData() {
	sdb.Data_file.Close()
}

func (sdb *SmallDB) WriteData(pos int64, ba []byte) int64 {
	if sdb.Debug > 30+1 {
		fmt.Println("WriteData")
	}
	var pos_res int64 = 0
	if pos == -1 {
		newPosition, err := sdb.Data_file.Seek(0, 2)
		if err != nil {
			fmt.Print("WriteData ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 30+3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	} else {
		newPosition, err := sdb.Data_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("WriteData ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 30+3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.Data_file.Write(byteSlice)
	if err != nil {
		fmt.Print("WriteData ")
		fmt.Println(err)
		os.Exit(-1)
		// log.Fatal(err)
	}
	if sdb.Debug > 30+3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return pos_res
}

func (sdb *SmallDB) ReadData(pos int64, len int) ([]byte, error) {
	if sdb.Debug > 30+1 {
		fmt.Println("ReadData")
	}
	if sdb.Config.UseSync > 0 {
		sdb.Data_file.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.Data_file.Seek(0, 2)
		if err != nil {
			fmt.Print("ReadData ")
			fmt.Println(err)
			return []byte{}, err
			//os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 30+3 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := sdb.Data_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("ReadData ")
			fmt.Println(err)
                        return []byte{}, err
			//os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 30+3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}
	byteSlice := make([]byte, len)
	bytesRead, err := sdb.Data_file.Read(byteSlice)
	if err != nil {
		fmt.Print("ReadData ")
		fmt.Println(err)
                return []byte{}, err
		// os.Exit(-1)
		// log.Fatal(err)
	}
	if sdb.Debug > 30+3 {
		fmt.Printf("Number of bytes read: %d\n", bytesRead)
		fmt.Printf("Data read: %s\n", byteSlice)
	}
	return byteSlice, nil
}

// free index data
func (sdb *SmallDB) OpenFreeIndexData(ind int) {
	s := fmt.Sprintf("%v", ind)
	// Open a new file for writing only
	file, err := os.OpenFile(
		sdb.Path+"/"+"free_index"+s+".bin",
		os.O_RDWR|os.O_CREATE,
		0666,
	) // |os.O_APPEND
	if err != nil {
		fmt.Print("OpenFreeIndexData ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	sdb.FreeIndexData_files[ind] = file
}

func (sdb *SmallDB) CloseFreeIndexData(ind int) {
	sdb.FreeIndexData_files[ind].Close()
}

func (sdb *SmallDB) WriteFreeIndexData(ind int, pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteFreeIndexData")
	}
	var pos_res int64 = 0
	if pos == -1 {
		newPosition, err := sdb.FreeIndexData_files[ind].Seek(0, 2)
		if err != nil {
			fmt.Print("WriteFreeIndexData ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	} else {
		newPosition, err := sdb.FreeIndexData_files[ind].Seek(pos, 0)
		if err != nil {
			fmt.Print("WriteFreeIndexData ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.FreeIndexData_files[ind].Write(byteSlice)
	if err != nil {
		fmt.Print("WriteFreeIndexData ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return pos_res
}

func (sdb *SmallDB) ReadFreeIndexData(ind int, pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadData")
	}
	if sdb.Config.UseSync > 0 {
		sdb.Data_file.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.FreeIndexData_files[ind].Seek(0, 2)
		if err != nil {
			fmt.Print("ReadData ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := sdb.FreeIndexData_files[ind].Seek(pos, 0)
		if err != nil {
			fmt.Print("ReadData ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}
	byteSlice := make([]byte, len)
	bytesRead, err := sdb.FreeIndexData_files[ind].Read(byteSlice)
	if err != nil {
		fmt.Print("ReadData ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Number of bytes read: %d\n", bytesRead)
		fmt.Printf("Data read: %s\n", byteSlice)
	}
	return byteSlice
}

// block
func (sdb *SmallDB) OpenBlock() {
	// Open a new file for writing only
	file, err := os.OpenFile(
		sdb.Path+"/"+sdb.Config.Blocks_file_name,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenBlock ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	sdb.Block_file = file
}

func (sdb *SmallDB) CloseBlock() {
	sdb.Block_file.Close()
}

func (sdb *SmallDB) WriteBlock(pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteBlock")
	}
	var pos_res int64 = 0
	if pos == -1 {
		newPosition, err := sdb.Block_file.Seek(0, 2)
		if err != nil {
			fmt.Print("WriteBlock ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	} else {
		newPosition, err := sdb.Block_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("WriteBlock ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		pos_res = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.Block_file.Write(byteSlice)
	if err != nil {
		fmt.Print("WriteBlock ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return pos_res
}

func (sdb *SmallDB) ReadBlock(pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadBlock")
	}
	if sdb.Config.UseSync > 0 {
		sdb.Block_file.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.Block_file.Seek(0, 2)
		if err != nil {
			fmt.Print("ReadBlock ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := sdb.Block_file.Seek(pos, 0)
		if err != nil {
			fmt.Print("ReadBlock ")
			fmt.Println(err)
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}

	byteSlice := make([]byte, len)
	bytesRead, err := sdb.Block_file.Read(byteSlice)
	if err != nil {
		fmt.Print("ReadBlock ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Number of bytes read: %d\n", bytesRead)
		fmt.Printf("Data read: %s\n", byteSlice)
	}
	return byteSlice
}

func (sdb *SmallDB) ReadBlocks(pos int64) ([]Block_struct, error) {
	res := []Block_struct{}
	pos_n := pos
	for i := 0; i < (int)(sdb.Config.Block_size); i++ {
		bab := sdb.ReadBlock(pos_n, Block_structLen)
		bs, _, err := To_Block(bab)
		if err != nil {
			return []Block_struct{}, err
		}
		pos_n = pos_n + Block_structLen
		res = append(res, bs)
	}
	return res, nil
}

func (sdb *SmallDB) WriteBlocks(pos int64, bsa []Block_struct) error {
	pos_n := pos
	for i := 0; i < (int)(sdb.Config.Block_size); i++ {
		bs := bsa[i]
		bab, _, err := From_Block(bs)
		if err != nil {
			return err
		}
		sdb.WriteBlock(pos_n, bab)
		pos_n = pos_n + Block_structLen
	}
	return nil
}

func (sdb *SmallDB) CreateDB(fields []string, path string) int {
	if sdb.Debug > 3 {
		fmt.Println("CreateDB begin")
	}
	if sdb.Inited {
		return -1 // база уже создана, для пересоздания надо СБРОСИТЬ это флаг
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// path not exists. check last path element
		pl := strings.Split(path, "/")
		if len(pl) > 1 {
			pn := strings.Join(pl[:len(pl)-1], "/")
			if _, err := os.Stat(pn); os.IsNotExist(err) {
				return -103
			}
			err := os.MkdirAll(pl[len(pl)-1], 0777)
			if err == nil || os.IsExist(err) {
				// return -101 //warum?
			} else {
				return -102 // What's going not right
			}
		} else {
			// this is name. create directory
			err := os.MkdirAll(path, 0777)
			if err == nil || os.IsExist(err) {
				return -101 //warum?
			} else {
				return -102 // What's going not right
			}
		}
	}
	var field_qty int32
	// удаляем старые файлы если они есть
	err := os.Remove(path + "/" + sdb.Config.Blocks_file_name)
	if err != nil {
		fmt.Println(err)
		//		return -11
	}
	err = os.Remove(path + "/" + sdb.Config.Data_file_name)
	if err != nil {
		fmt.Println(err)
	}
	for _, it := range sdb.Config.Index_files_name {
		err = os.Remove(path + "/" + it)
		if err != nil {
			fmt.Println(err)
		}
	}
	err = os.Remove(path + "/" + "config.json")
	if err != nil {
		fmt.Println(err)
	}
	// анализируем поля
	field_qty = (int32)(len(fields))
	if field_qty == 0 {
		return -2
	}
	if field_qty > 32 {
		return -3
	}
	sdb.Config.FieldsName = []string{}
	for j, it := range fields {
		// надо проверять на недопустимые символы
		str := strings.ToLower(it)
		matched, _ := regexp.MatchString(`^([a-z]|[а-я]|[0-9]|[_]){1,64}$`, str)
		if matched {
			sdb.Config.FieldsName = append(sdb.Config.FieldsName, it)
		} else {
			n := 64 + j
			return -n
		}
	}
	// открываем файл данных и записываем заголовок
	sdb.OpenData()
	dhs := Data_header_struct{}
	dhs.Id = 1
	dhs.Cnt = 0
	dhs.Field_qty = field_qty
	ba, _, err := From_Data_header(dhs)
	if err != nil {
		return -5 // ошибка
	}
	sdb.WriteData(-1, ba)
	// формируем файл с пустым блоком на все количество записей
	sdb.OpenBlock()
	bhs := Block_header_struct{}
	bhs.Id = 0
	bhs.PointerPrevBlock = 0
	bhs.PointerNextBlock = 0
	ba1, _, err := From_Block_header(bhs)
	if err != nil {
		return -6
	}
	sdb.WriteBlock(-1, ba1)
	// создаем индекс записей
	sdb.OpenIndex(ROW_INDEX_ID)
	ihs := Index_header_struct{}
	ihs.Id = ROW_INDEX_ID
	ihs.Mask = 0
	ihs.IsFree = 0
	ba2, _, err := From_Index_header(ihs)
	if err != nil {
		return -7
	}
	sdb.WriteIndex(ROW_INDEX_ID, -1, ba2)

	is := Index_struct{}
	is.Number = 0
	is.PointerFar = 0
	is.PointerNear = 0
	is.State = 0 // индексная запись пустая!
	// записываем пустые данные в блок
	ba3, _, err := From_Index(is)
	if err != nil {
		return -8
	}
	var i uint32
	for i = 0; i < sdb.Config.HashTable_size; i++ {
		sdb.WriteIndex(ROW_INDEX_ID, -1, ba3)
	}
	sdb.CloseIndex(ROW_INDEX_ID)
	if sdb.Config.UseDeletedData > 0 {
		// создаем файл и индекс для удаляемых записей
		// открываем файл данных и записываем заголовок
		sdb.OpenDeletedData()
		dhs := Data_header_struct{}
		dhs.Id = 1
		dhs.Cnt = 0
		dhs.Field_qty = field_qty
		ba, _, err := From_Data_header(dhs)
		if err != nil {
			return -9
		}
		sdb.WriteDeletedData(-1, ba)

		// создаем индекс записей
		sdb.OpenDeletedData_index()
		ihs := Index_header_struct{}
		ihs.Id = ROW_INDEX_ID
		ihs.Mask = 0
		ihs.IsFree = 0
		ba2, _, err := From_Index_header(ihs)
		if err != nil {
			return -11
		}
		sdb.WriteDeletedData_index(-1, ba2)

		is := Index_struct{}
		is.Number = 0
		is.PointerFar = 0
		is.PointerNear = 0
		is.State = 0 // индексная запись пустая!
		// записываем пустые данные в блок
		ba3, _, err := From_Index(is)
		if err != nil {
			return -12
		}
		var i uint32
		for i = 0; i < sdb.Config.HashTable_size; i++ {
			sdb.WriteDeletedData_index(-1, ba3)
		}
		sdb.CloseDeletedData_index()
		sdb.CloseDeletedData()
	}
	if sdb.Config.UseJournal > 0 {
		// создаем файл журнала
		sdb.OpenJournal()
	}
	sdb.CloseData()
	sdb.CloseBlock()
	sdb.Store_Config_SmallDB()
	if sdb.Debug > 3 {
		fmt.Printf("Store_Config_SmallDB done\r\n")
	}
	sdb.Inited = true
	return 0
}

func (sdb *SmallDB) Hash(key string) uint32 {
	var h uint32 = 0

	for _, p := range []byte(key) {
		h = h*HASHTAB_MUL + (uint32)(p)
	}
	return h % (uint32)(sdb.Config.HashTable_size)
}

func (sdb *SmallDB) CreateIndex(fields []string) error {
	var index_mask int64

	if sdb.Debug > 3 {
		fmt.Printf("fields %v\r\n", fields)
	}
	index_mask = int64(sdb.GetIndexIdByStringList(fields))
	if sdb.Debug > 3 {
		fmt.Printf("index_mask %v\r\n", index_mask)
	}
	sdb.Config.Index_files_name = append(sdb.Config.Index_files_name, "index"+fmt.Sprintf("%x", index_mask)+".bin")
	num := len(sdb.Config.Index_files_name) - 1
	sdb.OpenIndex(num)
	ihs := Index_header_struct{}
	ihs.Id = 0
	ihs.Mask = index_mask
	ihs.IsFree = 0
	ba1, _, err := From_Index_header(ihs)
	if err != nil {
		return err
	}
	sdb.WriteIndex(num, -1, ba1)

	is := Index_struct{}
	is.Number = 0
	is.PointerFar = 0
	is.PointerNear = 0
	is.State = 0 // индексная запись пустая!
	// записываем пустые данные в блок
	ba2, _, err := From_Index(is)
	if err != nil {
		return err
	}
	var i uint32
	for i = 0; i < sdb.Config.HashTable_size; i++ {
		sdb.WriteIndex(num, -1, ba2)
	}

	ic := Index_config{fields, false, index_mask}
	sdb.Config.Indexes = append(sdb.Config.Indexes, ic)

	inx := strings.Join(fields, ",")

	sdb.Config.IndexesMap[inx] = ic

	if false {
		sdb.OpenFreeIndexData(num)
		fidhs := FreeIndexData_header_struct{}
		fidhs.Id = 1
		fidhs.Cnt = 0
		ba, _, err := From_FreeIndexData_header(fidhs)
		if err != nil {
			return err
		}
		sdb.WriteFreeIndexData(num, -1, ba)
		sdb.CloseFreeIndexData(num)
	}

	sdb.CloseIndex(num)
	sdb.Store_Config_SmallDB()
	return nil
}

func uint64Hasher(algorithm hash.Hash64, text string) int64 {
	algorithm.Write([]byte(text))
	return int64(algorithm.Sum64())
}

func (sdb *SmallDB) CreateIndexFree(index_name string) error {
	sdb.Config.Index_files_name = append(sdb.Config.Index_files_name, "index_"+index_name+".bin")

	num_free := 0
	for i, _ := range sdb.Config.Indexes {
		ic := sdb.Config.Indexes[i]
		if ic.Free {
			num_free = num_free + 1
		}
	}

	num := len(sdb.Config.Index_files_name) + num_free - 1

	sdb.OpenIndex(num)
	ihs := Index_header_struct{}
	ihs.Id = 0
	ihs.IsFree = 1
	algorithm := fnv.New64a()
	ihs.Mask = uint64Hasher(algorithm, index_name)

	ba1, _, err := From_Index_header(ihs)
	if err != nil {
		return err
	}
	sdb.WriteIndex(num, -1, ba1)

	is := Index_struct{}
	is.Number = 0
	is.PointerFar = 0
	is.PointerNear = 0
	is.State = 0 // индексная запись пустая!
	// записываем пустые данные в блок
	ba2, _, err := From_Index(is)
	if err != nil {
		return err
	}
	var i uint32
	for i = 0; i < sdb.Config.HashTable_size; i++ {
		sdb.WriteIndex(num, -1, ba2)
	}

	sdb.OpenFreeIndexData(num)
	fidhs := FreeIndexData_header_struct{}
	fidhs.Id = 1
	fidhs.Cnt = 0
	ba, _, err := From_FreeIndexData_header(fidhs)
	if err != nil {
		return err
	}
	sdb.WriteFreeIndexData(num, -1, ba)
	sdb.CloseFreeIndexData(num)

	ic := Index_config{[]string{index_name}, true, int64(num)}
	sdb.Config.Indexes = append(sdb.Config.Indexes, ic)

	sdb.Config.IndexesMap[index_name] = ic

	sdb.CloseIndex(num)
	sdb.Store_Config_SmallDB()
	return nil
}

func (sdb *SmallDB) GetFreeIndexId(index_name string) int {
	algorithm := fnv.New64a()
	n := uint64Hasher(algorithm, index_name)
	res := -1
	for j, ihs := range sdb.IhsA {
		if (int)(ihs.Mask) == (int)(n) {
			res = j
			break
		}
	}
	return res
}

func (sdb *SmallDB) GetIndexIdByStringList(fields []string) int {
	var res int

	n := 0
	for k := 0; k < len(fields); k++ {
		str_l := fields[k]
		it := ""
		it = str_l
		// ищем в списке полей
		pos := -1
		for i, fn := range sdb.Config.FieldsName {
			if fn == it {
				pos = i
			}
		}
		if pos < 0 {
			return -k
		}
		n = n | (1 << (uint32)(pos))
		k = k + 1
	}

	if len(sdb.IhsA) > 0 {
		for j, ihs := range sdb.IhsA {
			if (int)(ihs.Mask) == n {
				res = j
				break
			}
		}
	} else {
		res = n
	}
	return res
}

func (sdb *SmallDB) GetIndexId(fields ...interface{}) int {
	var res int
	v := reflect.ValueOf(fields)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64:
		res = (int)(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		res = (int)(v.Uint())
	case reflect.String:
		// запятыми разделяются поля
		var str string = v.String()
		str_l := strings.Split(str, ",")
		n := 0
		for k, it := range str_l {
			// ищем в списке полей
			pos := -1
			for i, fn := range sdb.Config.FieldsName {
				if fn == it {
					pos = i
				}
			}
			if pos < 0 {
				return -k
			}
			n = n | (1 << (uint32)(pos))
		}
		for j, ihs := range sdb.IhsA {
			if (int)(ihs.Mask) == n {
				res = j
				break
			}
		}

	case reflect.Slice:
		object := reflect.ValueOf(v.Interface())
		n := 0
		for k := 0; k < v.Len(); k++ {
			str_l := reflect.ValueOf(object.Index(k).Interface())
			it := ""
			switch str_l.Kind() {
			case reflect.String:
				it = str_l.String()
			default:
				return -100
			}
			// ищем в списке полей
			pos := -1
			for i, fn := range sdb.Config.FieldsName {
				if fn == it {
					pos = i
				}
			}
			if pos < 0 {
				return -k
			}
			n = n | (1 << (uint32)(pos))
			k = k + 1
		}
		for j, ihs := range sdb.IhsA {
			if (int)(ihs.Mask) == n {
				res = j
				break
			}
		}
	default:
		fmt.Printf("unexpected type %T\r\n", fields)
		res = 0
	}
	return res
}

func (sdb *SmallDB) OpenDB() (int, error) {
	if !sdb.Inited {
		return -1, nil
	}
	// открываем файл данных и считываем заголовок
	sdb.OpenData()
	len_d_header := Data_header_structLen
	ba, err11 := sdb.ReadData(0, len_d_header)
	if err11 != nil {
		fmt.Printf("Error %v\r\n", err11)
		return -2, err11
	}
	dhs, err, err1 := To_Data_header(ba)
	if err < 0 {
		fmt.Printf("Error %v %v\r\n", err, err1)
		return err, err1
	}
	sdb.Dhs = dhs
	// формируем индексный файл на все количество записей
	sdb.OpenBlock()
	len_b_header := Block_header_structLen
	bab := sdb.ReadBlock(0, len_b_header)
	bhs, err, err1 := To_Block_header(bab)
	if err < 0 {
		fmt.Printf("Error %v\r\n", err, err1)
		return err, err1
	}
	sdb.Bhs = bhs
	// теоретически надо загрузить начальный блок?
	// открываем известные индексы
	for i, _ := range sdb.Config.Index_files_name {
		sdb.OpenIndex(i)
		len_i_header := Index_header_structLen
		bai := sdb.ReadIndex(i, 0, len_i_header)
		ihs, err, err1 := To_Index_header(bai)
		if err < 0 {
			fmt.Printf("Error %v %v\r\n", err, err1)
			return err, err1
		}
		sdb.IhsA = append(sdb.IhsA, ihs)
	}
	// загружаем индекс
	sdb.OpenIndex(ROW_INDEX_ID)
	len_i_header := Index_header_structLen
	bai := sdb.ReadIndex(ROW_INDEX_ID, 0, len_i_header)
	ihs, err, err1 := To_Index_header(bai)
	if err < 0 {
		fmt.Printf("Error %v %v\r\n", err, err1)
		return err, err1
	}
	sdb.RIIhs = ihs
	sdb.FreeIndexData_files = make(map[int]*os.File)

	if sdb.Config.UseDeletedData > 0 {
		// создаем файл и индекс для удаляемых записей
		// открываем файл данных и записываем заголовок
		sdb.OpenDeletedData()
		len_d_header := Data_header_structLen
		ba := sdb.ReadDeletedData(0, len_d_header)
		dhs, err, err1 := To_Data_header(ba)
		if err < 0 {
			fmt.Printf("Error %v %v\r\n", err, err1)
			return err, err1
		}
		sdb.DDhs = dhs
		sdb.OpenDeletedData_index()
		len_i_header := Index_header_structLen
		bai := sdb.ReadDeletedData_index(0, len_i_header)
		ihs, err, err1 := To_Index_header(bai)
		if err < 0 {
			fmt.Printf("Error %v %v\r\n", err, err1)
			return err, err1
		}
		sdb.DDIhs = ihs
	}
	if sdb.Config.UseJournal > 0 {
		// создаем файл журнала
		sdb.OpenJournal()
	}
	// открываем доступ к базе
	sdb.Opened = true
	return 0, nil
}

type IndexData struct {
	Data string
	Mask int64
	Pos  int64
}

func (sdb *SmallDB) MakeIndexData(ind int, inxd IndexData, pos_g int64) error {
	inx := sdb.Hash(inxd.Data)
	pos_inx := (int64)(Index_header_structLen + inx*Index_structLen)
	bai := sdb.ReadIndex(ind, pos_inx, Index_structLen)
	is, _, err := To_Index(bai)
	if err != nil {
		return err
	}
	// проверяем состояние индексной записи, если == 0 то пустая
	if is.State == 0 {
		// этого блока нет
		is.Number = (int64)(inx)
		// строим новый блок
		// первый из группы
		bs := Block_struct{}
		bs.Id = (int64)(inx)
		bs.PointerData = inxd.Pos
		bs.PointerFar = 0
		bs.PointerNear = 0
		ba2, _, err := From_Block(bs)
		if err != nil {
			return err
		}
		pos := sdb.WriteBlock(-1, ba2)
		// остальные из группы размер в sdb.Config.Block_size
		// записываем пустые данные в блок
		bs.Id = 0
		bs.PointerData = 0
		ba3, _, err := From_Block(bs)
		if err != nil {
			return err
		}
		for i := 1; i < (int)(sdb.Config.Block_size); i++ {
			sdb.WriteBlock(-1, ba3)
		}
		is.PointerFar = pos
		is.PointerNear = 0
		is.State = INDEX_USED
		bai, _, err = From_Index(is)
		if err != nil {
			return err
		}
		sdb.WriteIndex(ind, pos_inx, bai)
	} else {
		next_ptr := is.PointerFar
		first_ptr := next_ptr
		flag_break := false
		var bs_first Block_struct
		flag_use := true
		for {
			// такой блок уже есть считываем его
			// читаем группу блоков из из sdb.Config.Block_size
			bsa, err := sdb.ReadBlocks((int64)(next_ptr))
			if err != nil {
				return err
			}
			if next_ptr == first_ptr {
				bs_first = bsa[0]
				// смотрим, что в первом блоке - .PointerFar ненулевое значение.
				// переходим сразу к этому номеру блока
				if bsa[sdb.Config.Block_size-1].PointerFar != 0 {
					next_ptr = bsa[sdb.Config.Block_size-1].PointerFar
					flag_use = false
				} else {
				}
			} else {
				flag_use = true
			}
			if flag_use {
				// ищем конец и добавляем
				flag := true
				for j, bs := range bsa {
					if bs.PointerData == 0 {
						bsa[j].Id = (int64)(inx)
						bsa[j].PointerData = inxd.Pos
						bsa[j].PointerFar = 0
						bsa[j].PointerNear = 0
						if j > 0 {
							bsa[j-1].PointerNear = (int32)(j * Block_structLen)
						}
						sdb.WriteBlocks((int64)(next_ptr), bsa)
						flag = false
						flag_break = true
						break
					}
				}
				if flag {
					// проверяем, что в последнем блоке нет указателя
					if bsa[sdb.Config.Block_size-1].PointerFar != 0 {
						next_ptr = bsa[sdb.Config.Block_size-1].PointerFar
					} else {
						// строим новый блок
						bs := Block_struct{}
						bs.Id = (int64)(inx)
						bs.PointerData = inxd.Pos
						bs.PointerFar = 0
						bs.PointerNear = 0
						ba2, _, err := From_Block(bs)
						if err != nil {
							return err
						}
						// записываем первый элемент из группы sdb.Config.Block_size
						// записываем в конец файла
						pos := sdb.WriteBlock(-1, ba2)
						// записываем пустые данные в блок
						// формируем и ...
						bs.Id = 0
						bs.PointerData = 0
						ba3, _, err := From_Block(bs)
						if err != nil {
							return err
						}
						// записываем остальные пустые блоки
						for k := 1; k < (int)(sdb.Config.Block_size); k++ {
							sdb.WriteBlock(-1, ba3)
						}
						// корректируем в последнем блоке дальний указатель на позицию нового блока
						bsa[sdb.Config.Block_size-1].PointerFar = pos
						bsa[sdb.Config.Block_size-1].PointerNear = 0
						sdb.WriteBlocks((int64)(next_ptr), bsa)
						// и корректируем PointerFar в первом элементе самого первого блока
						bs_first.PointerFar = pos
						ba4, _, err := From_Block(bs_first)
						if err != nil {
							return err
						}
						sdb.WriteBlock(first_ptr, ba4)
						flag_break = true
					}
				}
			}
			if flag_break {
				break
			}
		}
	}
	return nil
}

func (sdb *SmallDB) Store_record_on_map(args map[string]string) (int64, int64, error) {
	//fmt.Printf("len(args) %v args %#v\r\n", len(args), args)
	args_list := make([]string, len(sdb.FieldsNameMap))
	for k, v := range args {
		fn, ok := sdb.FieldsNameMap[k]
		if !ok {
			fmt.Printf("k %v\r\n", k)
			return 0, -1000, nil
		}
		args_list[fn] = v
	}
	return sdb.Store_record_strings(args_list)
}

func (sdb *SmallDB) Store_record(args ...string) (int64, int64, error) {
	args_list := []string{}
	for i, _ := range args {
		args_list = append(args_list, args[i])
	}
	return sdb.Store_record_strings(args_list)
}

func (sdb *SmallDB) Store_record_strings(args []string) (int64, int64, error) {
	// возвращает либо отрицательное значение - ошибка, либо позицию записанных данных
	var result int64 = 0
	var num int64 = -1
	if !sdb.Inited {
		return -1, num, errors.New("Data base not inited")
	}
	if sdb.Opened {
		// открываем файл данных и считываем заголовок
		if len(args) == (int)(sdb.Dhs.Field_qty) {
			// формируем запись и параллельно индекс
			inx_data := []IndexData{}
			for _, _ = range sdb.IhsA {
				d := IndexData{}
				d.Data = ""
				d.Pos = 0
				inx_data = append(inx_data, d)
			}
			var pos_g int64 = 0
			if false {
				uuid_ := uuid.NewV4()
				/*
				uuid_, err := uuid.NewV4()
				if err != nil {
					fmt.Printf("Something went wrong: %s", err)
					return -1, -101, err
				}
				*/
				row_id := uuid_.Bytes()
				ds := Data_struct{}
				ds.Id = sdb.Cnt
				ds.State = 0
				ds.Field = -1 // RowID
				ds.DataLen = (int32)(len(row_id))
				ba, _, err := From_Data(ds)
				if err != nil {
					return -1, -1, err
				}
				pos := sdb.WriteData(-1, ba)
				pos_g = pos
				// присваиваем значение начала записи
				result = pos
				ba = []byte(row_id)
				sdb.WriteData(-1, ba)
			}
			for i, it := range args {
				ds := Data_struct{}
				ds.Id = sdb.Cnt
				ds.State = 0
				ds.Field = (int32)(i)
				// whats do if length of data is zero? 
				ds.DataLen = (int32)(len(it))
				ba, _, err := From_Data(ds)
				if err != nil {
					return -1, -1, err
				}
				pos := sdb.WriteData(-1, ba)
				if true {
					if i == 0 {
						pos_g = pos
						// присваиваем значение начала записи
						result = pos
					}
				}
				ba = []byte(it)
				sdb.WriteData(-1, ba)
				// надо найти индекс
				for j, ihs := range sdb.IhsA {
					n := (1 << (uint32)(i)) & ihs.Mask
					if n > 0 {
						inx_data[j].Data = inx_data[j].Data + " | " + it
						inx_data[j].Pos = pos_g
						inx_data[j].Mask = ihs.Mask
					}
				}
			}
			// добавление в RowIndex
			RowIndexData := fmt.Sprintf("%0d", sdb.Cnt)
			inxd_r := IndexData{}
			inxd_r.Data = RowIndexData
			inxd_r.Pos = pos_g
			sdb.MakeIndexData(ROW_INDEX_ID, inxd_r, pos_g)
			// добавление в остальные индексы
			for i, _ := range sdb.IhsA {
				inxd := inx_data[i]
				if len(inxd.Data) > 0 {
					sdb.MakeIndexData(i, inxd, pos_g)
				} else {
					// этого индекса нет. Пропускаем.
				}
			}
			num = sdb.Cnt
			sdb.Cnt = sdb.Cnt + 1
			// сохраняем счетчик записей
			sdb.Dhs.Cnt = sdb.Cnt
			ba, _, err := From_Data_header(sdb.Dhs)
			if err != nil {
				return -1, -1, err
			}
			sdb.WriteData(0, ba)
		}
	} else {
		return -5, num, errors.New("Data base not opened")
	}
	return result, num, nil
}

func (sdb *SmallDB) StoreFreeIndex(index_name string, index_data string, pos_g int64) (int64, error) {
	var result int64 = 0
	if !sdb.Inited {
		return -1, errors.New("Data base not inited")
	}
	if sdb.Opened {
		// открываем файл данных и считываем заголовок
		// формируем запись и параллельно индекс
		inx_data := []IndexData{}
		for _, _ = range sdb.IhsA {
			d := IndexData{}
			d.Data = ""
			d.Pos = 0
			inx_data = append(inx_data, d)
		}
		// найдем имя свободного индекса
		algorithm := fnv.New64a()
		ihs_Mask := uint64Hasher(algorithm, index_name)
		// надо найти индекс
		for j, ihs := range sdb.IhsA {
			if ihs.IsFree != 0 {
				if ihs.Mask == ihs_Mask {
					inx_data[j].Data = index_data
					inx_data[j].Pos = pos_g
					inx_data[j].Mask = ihs.Mask
				}
			}
		}
		for i, _ := range sdb.IhsA {
			inxd := inx_data[i]
			if len(inxd.Data) > 0 {
				sdb.MakeIndexData(i, inxd, pos_g)
			} else {
				// этого индекса нет. Пропускаем.
			}
		}
		// добавляем запись в свободный индекс
		result = sdb.Cnt
	} else {
		return -5, errors.New("Data base not opened")
	}
	return result, nil
}

func (sdb *SmallDB) Get_field_value_by_name(rec *Record, field_name string) (string, error) {
	//fmt.Printf("Get_field_value_by_name %#v %v\r\n", rec, field_name)
	fn, ok := sdb.FieldsNameMap[field_name]
	if !ok {
		return "", errors.New(fmt.Sprintf("Bad field name %v", field_name))
	}
	return rec.FieldsValue[fn], nil
}

func (sdb *SmallDB) Get_fields_value_with_name(rec *Record) ([][]string, error) {
	//fmt.Printf("Get_field_value_by_name %#v %v\r\n", rec, field_name)
	result := [][]string{}
	if len(rec.FieldsValue) != len(sdb.FieldsNameMap) {
		return result, errors.New("Number of fields in record not equal number of fields in database")
	}
	for k, v := range sdb.FieldsNameMap {
		result = append(result, []string{k, rec.FieldsValue[v]})
	}
	return result, nil
}

func (sdb *SmallDB) Find_record(ind int, args ...string) ([]*Record, int, error) {
	args_list := []string{}
	for i, _ := range args {
		args_list = append(args_list, args[i])
	}

	return sdb.Find_record_string_array(ind, args_list)
}

func (sdb *SmallDB) Find_record_index_string(index []string, args []string) ([]*Record, int, error) {
	ind := sdb.GetIndexIdByStringList(index)
	if sdb.Debug > 1 {
		fmt.Printf("ind %v\r\n", ind)
	}

	return sdb.Find_record_string_array(int(ind), args)
}

func (sdb *SmallDB) Find_record_string_array(ind int, args []string) ([]*Record, int, error) {
	data_res := []*Record{}
	if !sdb.Inited {
		return data_res, -1, errors.New("Data base not inited")
	}
	if !sdb.Opened {
		return data_res, -5, errors.New("Data base not opened")
	}
	// ищет информацию по индексу
	// формируем данные для поиска
	if ind >= 0 && ind < len(sdb.IhsA) {
		//mask := sdb.IhsA[ind].Mask
		cnt := 0
		inxd := ""
		flag := false
		if sdb.IhsA[ind].IsFree == 0 {
			for i := 0; i < (int)(sdb.Dhs.Field_qty); i++ {
				n := (1 << (uint32)(i)) & sdb.IhsA[ind].Mask
				if n > 0 {
					if len(args) > cnt {
						inxd = inxd + " | " + args[cnt]
						cnt = cnt + 1
					} else {
						// ошибка аргументов меньше чем индекс требует
						flag = true
					}
				}
				if flag {
					return nil, -6, errors.New("number of arguments less than needed of index")
				}
			}
		} else {
			if len(args) == 1 {
				inxd = args[0]
			} else {
				return nil, -4, errors.New("number of arguments more than needed of index")
			}
		}
		// сформировали, ищем
		inx := sdb.Hash(inxd)
		pos_inx := (int64)(Index_header_structLen + inx*Index_structLen)
		if sdb.Debug > 3 {
			fmt.Printf("inx %v read pos_inx %x \r\n", inx, pos_inx)
		}
		bai := sdb.ReadIndex(ind, pos_inx, Index_structLen)
		is, _, err := To_Index(bai)
		if err != nil {
			return data_res, -10, err
		}
		if sdb.Debug > 3 {
			fmt.Printf("is.Number %v\r\n", is.Number)
		}
		// проверяем, что блок используется
		if is.State != 0 {
			next_ptr := is.PointerFar
			// такой блок есть, считываем его
			for {
				if sdb.Debug > 3 {
					fmt.Printf("block next_ptr %x\r\n", next_ptr)
				}
				bsa, err := sdb.ReadBlocks((int64)(next_ptr))
				if err != nil {
					return data_res, -11, err
				}
				// ищем конец и добавляем
				flag_end_block := false
				for j, bs := range bsa {
					// читаем данные и проверяем на соответствие
					if bs.PointerData != 0 {
						flag := true
						cnt := 0
						ptr := bs.PointerData
						data := []string{}
						if sdb.Debug > 3 {
							fmt.Printf("data ptr %v j %v\r\n", ptr, j)
						}
						var num int64 = -1
						for i := 0; i < (int)(sdb.Dhs.Field_qty); i++ {
							len_header := Data_structLen
							ba, err11 := sdb.ReadData(ptr, len_header)
							if err11 != nil {
								fmt.Printf("Error %v\r\n", err11)
								return data_res, -12, err11
							}
							ds, err, err1 := To_Data(ba)
							if err < 0 {
								fmt.Printf("Error %v %v\r\n", err, err1)
								return data_res, -13, err1
							}
							ptr = ptr + (int64)(len_header)
							d := ""
							if ds.DataLen == 0 {
								// return nil, -7, nil
								data = append(data, "")
							} else {
								ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
								if err11 != nil {
									fmt.Printf("Error %v\r\n", err11)
									return data_res, -14, err11
								}
								d = string(ba)
								if sdb.Debug > 3 {
									fmt.Printf("ds %v d %v ptr %v\r\n", ds, d, ptr)
								}
				
								data = append(data, d)
							}
							num = ds.Id
							if sdb.IhsA[ind].IsFree == 0 {
								n := (1 << (uint32)(i)) & sdb.IhsA[ind].Mask
								if sdb.Debug > 3 {
									fmt.Printf("ds %v d %v ptr %v n %b\r\n", ds, d, ptr, n)
								}
								if n > 0 {
									if sdb.Debug > 3 {
										fmt.Printf("d '%v' args[cnt] '%v' d != args[cnt] %v\r\n", d, args[cnt], d != args[cnt])
									}
									if d != args[cnt] {
										flag = false
									}
									cnt = cnt + 1
								}
							}
							ptr = ptr + (int64)(ds.DataLen)
						}
						if flag {
							// create Record
							rec := Record{num, data}
							// добавляем в выборку
							data_res = append(data_res, &rec)
							// зачем прекращать выборку????
						}
					} else {
						// блок похоже без данных!
						flag_end_block = true
					}
				}
				if flag_end_block {
					break
				} else {
					next_ptr = bsa[sdb.Config.Block_size-1].PointerFar
					if next_ptr == 0 {
						break
					}
				}
			}
			return data_res, 0, nil
		}
	}
	return nil, -2, errors.New("no data")
}

func (sdb *SmallDB) Delete_record(rec int64) (int, error) {
	ind := ROW_INDEX_ID
	if sdb.Debug > 1 {
		fmt.Printf("ind %v\r\n", ind)
	}
	if !sdb.Inited {
		return -1, errors.New("Data base not inited")
	}
	if !sdb.Opened {
		return -5, errors.New("Data base not opened")
	}

	data_n := fmt.Sprintf("%v", rec)
	// сформировали, ищем
	inx := sdb.Hash(data_n)
	pos_inx := (int64)(Index_header_structLen + inx*Index_structLen)
	if sdb.Debug > 3 {
		fmt.Printf("ind %v read pos_inx %x \r\n", ind, pos_inx)
	}
	bai := sdb.ReadIndex(ind, pos_inx, Index_structLen)
	is, _, err := To_Index(bai)
	if err != nil {
		return -15, err
	}
	if sdb.Debug > 3 {
		fmt.Printf("is.Number %v is.State %v\r\n", is.Number, is.State)
	}
	// проверяем, что блок используется
	if is.State != 0 {
		next_ptr := is.PointerFar
		// такой блок есть, считываем его
		for {
			if sdb.Debug > 3 {
				fmt.Printf("block next_ptr %x\r\n", next_ptr)
			}
			bsa, err := sdb.ReadBlocks((int64)(next_ptr))
			if err != nil {
				return -16, err
			}
			// ищем конец и добавляем
			flag_end_block := false
			for j, bs := range bsa {
				// читаем данные и проверяем на соответствие
				if bs.PointerData != 0 {
					flag := true
					ptr := bs.PointerData
					if sdb.Debug > 3 {
						fmt.Printf("data ptr %v j %v\r\n", ptr, j)
					}
					for i := 0; i < (int)(sdb.Dhs.Field_qty); i++ {
						len_header := Data_structLen
						ba, err11 := sdb.ReadData(ptr, len_header)
						if err11 != nil {
							fmt.Printf("Error %v\r\n", err11)
							return -12, err11
						}
						ds, err, err1 := To_Data(ba)
						if err < 0 {
							fmt.Printf("Error %v %v\r\n", err, err1)
							return -17, err1
						}
						// меняем и сохранем
						ds.State = 1
						ba, _, err1 = From_Data(ds)
						if err1 != nil {
							return -17, err1
						}
						sdb.WriteData(ptr, ba)

						ptr = ptr + (int64)(len_header)
						if ds.DataLen == 0 {
							//return 0, nil
						} else {
							ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
							if err11 != nil {
								fmt.Printf("Error %v\r\n", err11)
								return -13, err11
							}
							d := string(ba)
							// data = append(data, d)
							if sdb.Debug > 3 {
								fmt.Printf("ds %v d %v ptr %v\r\n", ds, d, ptr)
							}
						}
						ptr = ptr + (int64)(ds.DataLen)
					}
					if flag {
						// помечаем блок
						bsa[j].PointerData = 0
						flag_end_block = true
					}
				} else {
					flag_end_block = true
				}
			}

			if flag_end_block {
				sdb.WriteBlocks((int64)(is.PointerFar), bsa)
				break
			} else {
				next_ptr = bsa[sdb.Config.Block_size-1].PointerFar
				if next_ptr == 0 {
					break
				}
			}
		}
		return 0, nil
	}
	return -2, errors.New("no data")
}

func (sdb *SmallDB) Load_records(rec int) ([]*Record, int, error) {
	data := []*Record{}
	if !sdb.Inited {
		return data, -1, errors.New("Data base not inited")
	}
	if sdb.Opened {
		ptr := (int64)(Data_header_structLen)
		// открываем файл данных и считываем по очереди
		for j := 0; j < rec; j++ {
			if sdb.Debug > 5 {
				fmt.Printf("current rec %v\r\n", j)
			}
			var i int32
			var num int64
			data_r := []string{}
			if sdb.Debug > 6 {
				fmt.Printf("sdb.Dhs.Field_qty %v\r\n", sdb.Dhs.Field_qty)
			}
			for i = 0; i < sdb.Dhs.Field_qty; i++ {
				len_header := Data_structLen
				ba, err11 := sdb.ReadData(ptr, len_header)
				if err11 != nil {
					fmt.Printf("Error %v\r\n", err11)
					return data, -12, err11
				}
				if sdb.Debug > 9 {
					fmt.Printf("ba %v\r\n", ba)
				}

				ds, err, err1 := To_Data(ba)
				if err < 0 {
					fmt.Printf("Error %v %v\r\n", err, err1)
				}
				if sdb.Debug > 7 {
					fmt.Printf("ds %#v\r\n", ds)
				}

				ptr = ptr + (int64)(len_header)
				if ds.State == 0 {
					if ds.DataLen == 0 {
						// it is not error - just no data
						// return nil, 0, nil
						data_r = append(data_r, "")
					} else {
						ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
						if err11 != nil {
							fmt.Printf("Error %v\r\n", err11)
							return data, -13, err11
						}
						if sdb.Debug > 9 {
							fmt.Printf("ds ba %v\r\n", ba)
						}

						d := string(ba)
						data_r = append(data_r, d)
					}
					num = ds.Id
				}
				ptr = ptr + (int64)(ds.DataLen)
			}
			if len(data_r) > 0 {
				rec := Record{num, data_r}
				data = append(data, &rec)
				if sdb.Debug > 5 {
					fmt.Printf("data %v\r\n", data)
				}
			}
		}
	} else {
		return data, -5, errors.New("Data base not opened")
	}
	return data, 0, nil
}

func (sdb *SmallDB) Load_record(rec int64) ([]*Record, int, error) {
	data := []*Record{}
	ind := ROW_INDEX_ID
	if sdb.Debug > 1 {
		fmt.Printf("ind %v\r\n", ind)
	}
	if !sdb.Inited {
		return data, -1, errors.New("data base not inited")
	}
	if !sdb.Opened {
		return data, -5, errors.New("data base not opened")
	}

	data_n := fmt.Sprintf("%v", rec)
	// сформировали, ищем
	inx := sdb.Hash(data_n)
	pos_inx := (int64)(Index_header_structLen + inx*Index_structLen)
	if sdb.Debug > 3 {
		fmt.Printf("ind %v read pos_inx %x \r\n", ind, pos_inx)
	}
	bai := sdb.ReadIndex(ind, pos_inx, Index_structLen)
	is, _, err := To_Index(bai)
	if err != nil {
		return data, -15, err
	}
	if sdb.Debug > 3 {
		fmt.Printf("is.Number %v is.State %v\r\n", is.Number, is.State)
	}
	// проверяем, что блок используется
	if is.State != 0 {
		next_ptr := is.PointerFar
		// такой блок есть, считываем его
		for {
			if sdb.Debug > 3 {
				fmt.Printf("block next_ptr %x\r\n", next_ptr)
			}
			bsa, err := sdb.ReadBlocks((int64)(next_ptr))
			if err != nil {
				return data, -16, err
			}
			// ищем конец и добавляем
			flag_end_block := false
			for j, bs := range bsa {
				// читаем данные и проверяем на соответствие
				if bs.PointerData != 0 {
					//flag := true
					ptr := bs.PointerData
					var num int64
					data_r := []string{}
					if sdb.Debug > 3 {
						fmt.Printf("data ptr %v j %v\r\n", ptr, j)
					}
					for i := 0; i < (int)(sdb.Dhs.Field_qty); i++ {
						len_header := Data_structLen
						ba, err11 := sdb.ReadData(ptr, len_header)
						if err11 != nil {
							fmt.Printf("Error %v\r\n", err11)
							return data, -12, err11
						}
						ds, err, err1 := To_Data(ba)
						if err < 0 {
							fmt.Printf("Error %v %v\r\n", err, err1)
							return data, -17, err1
						}
						if sdb.Debug > 7 {
							fmt.Printf("ds %#v\r\n", ds)
						}
						ptr = ptr + (int64)(len_header)
						if ds.State == 0 {
							if ds.DataLen == 0 {
								// it is not error - just no data
								data_r = append(data_r, "")
							} else {
								ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
								if err11 != nil {
									fmt.Printf("Error %v\r\n", err11)
									return data, -13, err11
								}
								if sdb.Debug > 9 {
									fmt.Printf("ds ba %v\r\n", ba)
								}

								d := string(ba)
								data_r = append(data_r, d)
							}
							num = ds.Id
						}
						ptr = ptr + (int64)(ds.DataLen)
					}
					if num == rec {
						if len(data_r) > 0 {
							rec := Record{num, data_r}
							data = append(data, &rec)
							if sdb.Debug > 5 {
								fmt.Printf("data %v\r\n", data)
							}
						}
					}
				} else {
					flag_end_block = true
				}
			}
			if flag_end_block {
				break
			} else {
				next_ptr = bsa[sdb.Config.Block_size-1].PointerFar
				if next_ptr == 0 {
					break
				}
			}
		}
		return data, 0, nil
	}
	return data, -2, errors.New("no data")
}

func (sdb *SmallDB) Load_lazy_records(rec int) (func()(*Record, int, error), error) {
	// data := []*Record{}
	if !sdb.Inited {
		return nil, errors.New("Data base not inited")
	}
	if sdb.Opened {
		ptr := (int64)(Data_header_structLen)
		// открываем файл данных и считываем по очереди
                j := 0
//		for j := 0; j < rec; j++ {
		lazy_load := func() (*Record, int, error) { 
                        var data *Record
			if sdb.Debug > 5 {
				fmt.Printf("current rec %v\r\n", j)
			}
			var i int32
			var num int64
			data_r := []string{}
			if sdb.Debug > 6 {
				fmt.Printf("sdb.Dhs.Field_qty %v\r\n", sdb.Dhs.Field_qty)
			}
			for i = 0; i < sdb.Dhs.Field_qty; i++ {
				len_header := Data_structLen
				ba, err11 := sdb.ReadData(ptr, len_header)
				if err11 != nil {
					fmt.Printf("Error %v\r\n", err11)
					return data, -12, err11
				}
				if sdb.Debug > 9 {
					fmt.Printf("ba %v\r\n", ba)
				}

				ds, err, err1 := To_Data(ba)
				if err < 0 {
					fmt.Printf("Error %v %v\r\n", err, err1)
				}
				if sdb.Debug > 7 {
					fmt.Printf("ds %#v\r\n", ds)
				}

				ptr = ptr + (int64)(len_header)
				if ds.State == 0 {
					if ds.DataLen == 0 {
						// it is not error - just no data
						// return nil, 0, nil
						data_r = append(data_r, "")
					} else {
						ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
						if err11 != nil {
							fmt.Printf("Error %v\r\n", err11)
							return data, -13, err11
						}
						if sdb.Debug > 9 {
							fmt.Printf("ds ba %v\r\n", ba)
						}

						d := string(ba)
						data_r = append(data_r, d)
					}
					num = ds.Id
				}
				ptr = ptr + (int64)(ds.DataLen)
			}
			if len(data_r) > 0 {
				rec := Record{num, data_r}
				data = &rec
				if sdb.Debug > 5 {
					fmt.Printf("data %v\r\n", data)
				}
			}
			return data, 0, nil
		}
		return lazy_load, nil
//		}
	} else {
		return nil, errors.New("Data base not opened")
	}
	return nil, nil
}
