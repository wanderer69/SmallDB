package small_db

import (
	"fmt"
	"io/ioutil"

	"strings"

	"bytes"
	"encoding/json"

	"encoding/binary"
	"errors"
	"hash"
	"hash/fnv"
	"os"
	"reflect"

	"regexp"

	uuid "github.com/satori/go.uuid"

	"github.com/wanderer69/SmallDB/internal/common"
)

const HashTabSize = 0x3FFFF
const HashTabMul = 31

type IndexConfig struct {
	FieldsName []string `json:"fields_name"`
	Free       bool
	Mask       int64
}

type SmallDBConfig struct {
	DataFileName             string                 `json:"data_file_name"`
	IndexFilesName           []string               `json:"index_files_name"`
	FreeIndexFilesName       []string               `json:"free_index_files_name"`
	BlocksFileName           string                 `json:"blocks_file_name"`
	DeletedDataFileName      string                 `json:"deleted_data_file_name"`
	DeletedDataIndexFileName string                 `json:"deleted_data_index_file_name"`
	JournalFileName          string                 `json:"journal_file_name"`
	RowIndexFileName         string                 `json:"row_index_file_name"`
	BlockSize                int32                  `json:"block_size"`
	HashTableSize            uint32                 `json:"hash_table_size"`
	UseSync                  int8                   `json:"use_sync"`
	UseJournal               int8                   `json:"use_journal"`
	UseDeletedData           int8                   `json:"use_deleted_data"`
	DatabaseName             string                 `json:"database_name"`
	FieldsName               []string               `json:"fields_name"`
	Indexes                  []IndexConfig          `json:"indexes"`
	IndexesMap               map[string]IndexConfig `json:"indexes_map"`
}

type SmallDB struct {
	Config               *SmallDBConfig
	Path                 string
	Inited               bool // если false - то значит не создана
	Opened               bool // если false - то значит не открыта
	RowIndexFile         *os.File
	IndexFiles           []*os.File
	DataFile             *os.File
	BlockFile            *os.File
	DeletedDataFile      *os.File
	DeletedDataIndexFile *os.File
	JournalFile          *os.File
	FreeIndexDataFiles   map[int]*os.File
	Debug                int
	Dhs                  DataHeaderStruct
	DDhs                 DataHeaderStruct
	Bhs                  BlockHeaderStruct
	IhsA                 []IndexHeaderStruct
	RIIhs                IndexHeaderStruct
	DDIhs                IndexHeaderStruct
	FIDhsA               map[int]FreeIndexDataHeaderStruct
	FieldsNameMap        map[string]int
	Cnt                  int64
}

func InitSmallDB(path string) *SmallDB {
	sdb := SmallDB{}
	createConfig := func() {
		sdbc := SmallDBConfig{
			DataFileName: "data.bin",
			// индексов нет.
			// sdbc.Index_files_name = append(sdbc.Index_files_name, "index0.bin")
			BlocksFileName:           "blocks.bin",
			DeletedDataFileName:      "deleted.bin",
			DeletedDataIndexFileName: "deleted_inx.bin",
			JournalFileName:          "journal.bin",
			RowIndexFileName:         "row_index.bin",
			BlockSize:                22,
			HashTableSize:            HashTabSize,
			DatabaseName:             "database",
			IndexesMap:               make(map[string]IndexConfig),
		}
		sdb.Config = &sdbc
		sdb.Inited = false
	}
	file, err := ioutil.ReadFile(path + "/" + "config.json")
	if err != nil {
		createConfig()
	} else {
		sdb.Inited = true
		err = json.Unmarshal([]byte(file), &sdb.Config)
		if err != nil {
			createConfig()
		}
	}
	sdb.FieldsNameMap = make(map[string]int)
	for i := range sdb.Config.FieldsName {
		sdb.FieldsNameMap[sdb.Config.FieldsName[i]] = i
	}
	sdb.Path = path
	sdb.DataFile = nil
	sdb.BlockFile = nil
	sdb.Debug = 0
	sdb.Cnt = 0
	sdb.Opened = false
	return &sdb
}

func (sdb *SmallDB) StoreConfigSmallDB() error {
	ba, _ := json.MarshalIndent(sdb.Config, "", "  ")
	err := os.Chmod(sdb.Path+"/"+"config.json", 0777)
	if err != nil {
		fmt.Println(err)
		// skip  error
	}
	err1 := ioutil.WriteFile(sdb.Path+"/"+"config.json", ba, 0777)
	if err1 != nil {
		fmt.Println(err1)
		os.Exit(-1)
	}
	return nil
}

type IndexHeaderStruct struct {
	Id     int64 // идентификатор индекса
	Mask   int64 // маска индекса (каждый бит это признак присутствия в индексе поля)
	IsFree uint16
}

const IndexHeaderStructLen = 16 + 2

type IndexStruct struct {
	Number      int64 // идентификатор индексной записи
	PointerFar  int64 // указатель на блок
	PointerNear int32 // указатель внутри блока
	State       int16 // признак состояния блока     (введено для исправления дефекта определения незанятого индекса)
}

const IndexUsed = 1
const IndexStructLen = 8 + 8 + 4 + 2

type BlockHeaderStruct struct {
	Id               int64 // идентификатор индекса
	PointerNextBlock int64 // идентификатор на следующий блок
	PointerPrevBlock int64 // идентификатор на предыдущий блок
	Size             int32 // размер блока
}

const BlockHeaderStructLen = 8 + 8 + 8 + 4

type BlockStruct struct {
	Id          int64 // идентификатор индекса
	PointerData int64 // указатель на данные
	PointerFar  int64 // указатель на следующую запись дальний (на блок)
	PointerNear int32 // указатель на следующую запись внутри блока
}

const BlockStructLen = 8 + 8 + 8 + 4

type DataHeaderStruct struct {
	Id        int64 // идентификатор данных
	Cnt       int64 // счетчик номера записи
	Field_qty int32 // количество полей в одной записи
}

const DataHeaderStructLen = 8 + 8 + 4

type DataStruct struct {
	Id      int64 // идентификатор записи
	State   int16 // статус записи 0 - сохранена 1 - требует удаления
	Field   int32 // номер поля -1 - RowID
	DataLen int32 // длина строки с данными
}

const DataStructLen = 8 + 2 + 4 + 4

type FreeIndexDataHeaderStruct struct {
	Id  int64 // идентификатор индекса файла значений свободного индекса
	Cnt int64 // счетчик номера записи
}

const FreeIndexDataHeaderStructLen = 8 + 8

type FreeIndexDataStruct struct {
	Id      int64 // идентификатор записи
	State   int16 // статус записи 0 - сохранена 1 - требует удаления
	DataLen int32 // длина строки с данными
}

const FreeIndexDataStructLen = 8 + 2 + 4 + 4

func FromIndexHeader(ihs IndexHeaderStruct) ([]byte, int, error) {
	// длина
	lenHeader := IndexHeaderStructLen
	var buf = bytes.NewBuffer(make([]byte, 0, lenHeader))
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &ihs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	return buf.Bytes(), lenHeader, nil
}

func ToIndexHeader(b_in []byte) (IndexHeaderStruct, int, error) {
	var ihs IndexHeaderStruct
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
	len_header := IndexHeaderStructLen
	return ihs, len_header, nil
}

func FromIndex(is IndexStruct) ([]byte, int, error) {
	// длина
	lenHeader := IndexStructLen
	var buf = bytes.NewBuffer(make([]byte, 0, lenHeader))
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &is); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	return buf.Bytes(), lenHeader, nil
}

func ToIndex(b_in []byte) (IndexStruct, int, error) {
	var is IndexStruct
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
	len_header := IndexStructLen
	return is, len_header, nil
}

func FromBlockHeader(bhs BlockHeaderStruct) ([]byte, int, error) {
	// длина
	lenHeader := BlockHeaderStructLen
	var buf = bytes.NewBuffer(make([]byte, 0, lenHeader))
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &bhs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), lenHeader, nil
}

func ToBlockHeader(b_in []byte) (BlockHeaderStruct, int, error) {
	var bhs BlockHeaderStruct
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
	len_header := BlockHeaderStructLen
	return bhs, len_header, nil
}

func FromBlock(bs BlockStruct) ([]byte, int, error) {
	// длина
	lenHeader := BlockHeaderStructLen
	var buf = bytes.NewBuffer(make([]byte, 0, lenHeader))
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &bs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	return buf.Bytes(), lenHeader, nil
}

func ToBlock(b_in []byte) (BlockStruct, int, error) {
	var bs BlockStruct
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
	len_header := BlockStructLen
	return bs, len_header, nil
}

func FromDataHeader(dhs DataHeaderStruct) ([]byte, int, error) {
	lenHeader := DataHeaderStructLen
	bIn := make([]byte, 0, lenHeader)
	var buf = bytes.NewBuffer(bIn)
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &dhs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), lenHeader, nil
}

func ToDataHeader(b_in []byte) (DataHeaderStruct, int, error) {
	var dhs DataHeaderStruct
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
	len_header := DataHeaderStructLen
	return dhs, len_header, nil
}

func FromData(ds DataStruct) ([]byte, int, error) {
	lenHeader := DataStructLen
	bIn := make([]byte, 0, lenHeader)
	var buf = bytes.NewBuffer(bIn)
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &ds); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), lenHeader, nil
}

func ToData(b_in []byte) (DataStruct, int, error) {
	var ds DataStruct
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
	len_header := DataStructLen
	return ds, len_header, nil
}

func FromFreeIndexDataHeader(dhs FreeIndexDataHeaderStruct) ([]byte, int, error) {
	lenHeader := FreeIndexDataHeaderStructLen
	bIn := make([]byte, 0, lenHeader)
	var buf = bytes.NewBuffer(bIn)
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &dhs); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), lenHeader, nil
}

func ToFreeIndexDataHeader(b_in []byte) (FreeIndexDataHeaderStruct, int, error) {
	var dhs FreeIndexDataHeaderStruct
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
	len_header := FreeIndexDataHeaderStructLen
	return dhs, len_header, nil
}

func FromFreeIndexData(ds FreeIndexDataStruct) ([]byte, int, error) {
	lenHeader := FreeIndexDataStructLen
	bIn := make([]byte, 0, lenHeader)
	var buf = bytes.NewBuffer(bIn)
	// Unpacked to Packed
	if err := binary.Write(buf, binary.LittleEndian, &ds); err != nil {
		fmt.Println(err)
		return []byte{}, -1, err
	}
	// длина
	return buf.Bytes(), lenHeader, nil
}

func ToFreeIndexData(b_in []byte) (FreeIndexDataStruct, int, error) {
	var ds FreeIndexDataStruct
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
	len_header := FreeIndexDataStructLen
	return ds, len_header, nil
}

const RowIndexID = 0xFFFF0000

// delete data index
func (sdb *SmallDB) OpenDeletedDataIndex() {
	// Open a new file for writing only
	file, err := os.OpenFile(
		sdb.Path+"/"+sdb.Config.DeletedDataIndexFileName,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenDeletedData_index ")
		fmt.Println(err)
		os.Exit(-1)
	}
	sdb.DeletedDataIndexFile = file
}

func (sdb *SmallDB) CloseDeletedDataIndex() {
	sdb.DeletedDataIndexFile.Close()
}

func (sdb *SmallDB) WriteDeletedDataIndex(pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteDeletedData_index")
	}
	var posRes int64 = 0
	if pos == -1 {
		newPosition, err := sdb.DeletedDataIndexFile.Seek(0, 2)
		if err != nil {
			fmt.Print("WriteDeletedData_index ")
			fmt.Println(err)
			// log.Fatal(err)
			os.Exit(-1)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		posRes = newPosition
	} else {
		newPosition, err := sdb.DeletedDataIndexFile.Seek(pos, 0)
		if err != nil {
			fmt.Print("WriteDeletedData_index ")
			fmt.Println(err)
			//log.Fatal(err)
			os.Exit(-1)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		posRes = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.DeletedDataIndexFile.Write(byteSlice)
	if err != nil {
		fmt.Print("WriteDeletedData_index ")
		//log.Fatal(err)
		fmt.Println(err)
		os.Exit(-1)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return posRes
}

func (sdb *SmallDB) ReadDeletedDataIndex(pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadDeletedData_index")
	}
	if sdb.Config.UseSync > 0 {
		sdb.JournalFile.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.DeletedDataIndexFile.Seek(0, 2)
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
		newPosition, err := sdb.DeletedDataIndexFile.Seek(pos, 0)
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
	bytesRead, err := sdb.DeletedDataIndexFile.Read(byteSlice)
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
		sdb.Path+"/"+sdb.Config.DeletedDataFileName,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenDeletedData ")
		fmt.Println(err)
		os.Exit(-1)
		// log.Fatal(err)
	}
	sdb.DeletedDataFile = file
}

func (sdb *SmallDB) CloseDeletedData() {
	sdb.DeletedDataFile.Close()
}

func (sdb *SmallDB) WriteDeletedData(pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteDeletedData")
	}
	var pos_res int64 = 0
	if pos == -1 {
		newPosition, err := sdb.DeletedDataFile.Seek(0, 2)
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
		newPosition, err := sdb.DeletedDataFile.Seek(pos, 0)
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
	bytesWritten, err := sdb.DeletedDataFile.Write(byteSlice)
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
		sdb.JournalFile.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.DeletedDataFile.Seek(0, 2)
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
		newPosition, err := sdb.DeletedDataFile.Seek(pos, 0)
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
	bytesRead, err := sdb.DeletedDataFile.Read(byteSlice)
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
		sdb.Path+"/"+sdb.Config.JournalFileName,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenJournal ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	sdb.JournalFile = file
}

func (sdb *SmallDB) CloseJournal() {
	sdb.JournalFile.Close()
}

func (sdb *SmallDB) WriteJournal(pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteJournal")
	}
	var posRes int64 = 0
	if pos == -1 {
		newPosition, err := sdb.JournalFile.Seek(0, 2)
		if err != nil {
			fmt.Print("WriteJournal ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		posRes = newPosition
	} else {
		newPosition, err := sdb.JournalFile.Seek(pos, 0)
		if err != nil {
			fmt.Print("WriteJournal ")
			fmt.Println(err)
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		posRes = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.JournalFile.Write(byteSlice)
	if err != nil {
		fmt.Print("WriteJournal ")
		fmt.Println(err)
		os.Exit(-1)
		// log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return posRes
}

func (sdb *SmallDB) ReadJournal(pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadJournal")
	}
	if sdb.Config.UseSync > 0 {
		sdb.JournalFile.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.JournalFile.Seek(0, 2)
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
		newPosition, err := sdb.JournalFile.Seek(pos, 0)
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
	bytesRead, err := sdb.JournalFile.Read(byteSlice)
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
func (sdb *SmallDB) OpenIndex(indexID int) int {
	// Open a new file for writing only
	fName := ""
	if indexID == RowIndexID {
		fName = sdb.Path + "/" + sdb.Config.RowIndexFileName
	} else {
		fName = sdb.Path + "/" + sdb.Config.IndexFilesName[indexID]
	}
	file, err := os.OpenFile(
		fName,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenIndex ")
		fmt.Println(err)
		os.Exit(-1)
		//log.Fatal(err)
	}
	if indexID == RowIndexID {
		sdb.RowIndexFile = file
	} else {
		sdb.IndexFiles = append(sdb.IndexFiles, file)
	}
	return indexID
}

func (sdb *SmallDB) CloseIndex(indexID int) {
	if indexID == RowIndexID {
		sdb.RowIndexFile.Close()
	} else {
		sdb.IndexFiles[indexID].Close()
	}
}

func (sdb *SmallDB) WriteIndex(indexID int, pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteIndex")
	}
	var f *os.File
	if indexID == RowIndexID {
		f = sdb.RowIndexFile
	} else {
		f = sdb.IndexFiles[indexID]
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

func (sdb *SmallDB) ReadIndex(indexID int, pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadIndex")
	}
	var f *os.File
	if indexID == RowIndexID {
		f = sdb.RowIndexFile
	} else {
		f = sdb.IndexFiles[indexID]
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
		sdb.Path+"/"+sdb.Config.DataFileName,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		fmt.Print("OpenData ")
		fmt.Println(err)
		os.Exit(-1)
		// log.Fatal(err)
	}
	sdb.DataFile = file
}

func (sdb *SmallDB) CloseData() {
	sdb.DataFile.Close()
}

func (sdb *SmallDB) WriteData(pos int64, ba []byte) int64 {
	if sdb.Debug > 30+1 {
		fmt.Println("WriteData")
	}
	var pos_res int64 = 0
	if pos == -1 {
		newPosition, err := sdb.DataFile.Seek(0, 2)
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
		newPosition, err := sdb.DataFile.Seek(pos, 0)
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
	bytesWritten, err := sdb.DataFile.Write(byteSlice)
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
		sdb.DataFile.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.DataFile.Seek(0, 2)
		if err != nil {
			if sdb.Debug > 30+2 {
				fmt.Print("ReadData ")
				fmt.Println(err)
			}
			return []byte{}, err
			//os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 30+3 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := sdb.DataFile.Seek(pos, 0)
		if err != nil {
			if sdb.Debug > 30+2 {
				fmt.Print("ReadData ")
				fmt.Println(err)
			}
			return []byte{}, err
			//os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 30+3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}
	byteSlice := make([]byte, len)
	bytesRead, err := sdb.DataFile.Read(byteSlice)
	if err != nil {
		if sdb.Debug > 30+2 {
			fmt.Print("ReadData ")
			fmt.Println(err)
		}
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
		if sdb.Debug > 30+5 {
			fmt.Print("OpenFreeIndexData ")
			fmt.Println(err)
		}
		os.Exit(-1)
		//log.Fatal(err)
	}
	sdb.FreeIndexDataFiles[ind] = file
}

func (sdb *SmallDB) CloseFreeIndexData(ind int) {
	sdb.FreeIndexDataFiles[ind].Close()
}

func (sdb *SmallDB) WriteFreeIndexData(ind int, pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteFreeIndexData")
	}
	var posRes int64 = 0
	if pos == -1 {
		newPosition, err := sdb.FreeIndexDataFiles[ind].Seek(0, 2)
		if err != nil {
			if sdb.Debug > 30+6 {
				fmt.Print("WriteFreeIndexData ")
				fmt.Println(err)
			}
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		posRes = newPosition
	} else {
		newPosition, err := sdb.FreeIndexDataFiles[ind].Seek(pos, 0)
		if err != nil {
			if sdb.Debug > 30+2 {
				fmt.Print("WriteFreeIndexData ")
				fmt.Println(err)
			}
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		posRes = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.FreeIndexDataFiles[ind].Write(byteSlice)
	if err != nil {
		if sdb.Debug > 30+2 {
			fmt.Print("WriteFreeIndexData ")
			fmt.Println(err)
		}
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return posRes
}

func (sdb *SmallDB) ReadFreeIndexData(ind int, pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadFreeIndexData")
	}
	if sdb.Config.UseSync > 0 {
		sdb.DataFile.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.FreeIndexDataFiles[ind].Seek(0, 2)
		if err != nil {
			if sdb.Debug > 30+2 {
				fmt.Print("ReadFreeIndexData ")
				fmt.Println(err)
			}
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := sdb.FreeIndexDataFiles[ind].Seek(pos, 0)
		if err != nil {
			if sdb.Debug > 30+2 {
				fmt.Print("ReadFreeIndexData ")
				fmt.Println(err)
			}
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}
	byteSlice := make([]byte, len)
	bytesRead, err := sdb.FreeIndexDataFiles[ind].Read(byteSlice)
	if err != nil {
		if sdb.Debug > 30+2 {
			fmt.Print("ReadFreeIndexData ")
			fmt.Println(err)
		}
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
		sdb.Path+"/"+sdb.Config.BlocksFileName,
		os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		if sdb.Debug > 30+2 {
			fmt.Print("OpenBlock ")
			fmt.Println(err)
		}
		os.Exit(-1)
		//log.Fatal(err)
	}
	sdb.BlockFile = file
}

func (sdb *SmallDB) CloseBlock() {
	sdb.BlockFile.Close()
}

func (sdb *SmallDB) WriteBlock(pos int64, ba []byte) int64 {
	if sdb.Debug > 1 {
		fmt.Println("WriteBlock")
	}
	var posRes int64 = 0
	if pos == -1 {
		newPosition, err := sdb.BlockFile.Seek(0, 2)
		if err != nil {
			if sdb.Debug > 30+2 {
				fmt.Print("WriteBlock ")
				fmt.Println(err)
			}
			os.Exit(-1)
			// log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		posRes = newPosition
	} else {
		newPosition, err := sdb.BlockFile.Seek(pos, 0)
		if err != nil {
			if sdb.Debug > 30+2 {
				fmt.Print("WriteBlock ")
				fmt.Println(err)
			}
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
		posRes = newPosition
	}
	// Write bytes to file
	byteSlice := ba
	bytesWritten, err := sdb.BlockFile.Write(byteSlice)
	if err != nil {
		if sdb.Debug > 30+2 {
			fmt.Print("WriteBlock ")
			fmt.Println(err)
		}
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Wrote %d bytes.\n", bytesWritten)
	}
	return posRes
}

func (sdb *SmallDB) ReadBlock(pos int64, len int) []byte {
	if sdb.Debug > 1 {
		fmt.Println("ReadBlock")
	}
	if sdb.Config.UseSync > 0 {
		sdb.BlockFile.Sync()
	}
	if pos == -1 {
		newPosition, err := sdb.BlockFile.Seek(0, 2)
		if err != nil {
			if sdb.Debug > 30+2 {
				fmt.Print("ReadBlock ")
				fmt.Println(err)
			}
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	} else {
		newPosition, err := sdb.BlockFile.Seek(pos, 0)
		if err != nil {
			if sdb.Debug > 30+2 {
				fmt.Print("ReadBlock ")
				fmt.Println(err)
			}
			os.Exit(-1)
			//log.Fatal(err)
		}
		if sdb.Debug > 3 {
			fmt.Println("Just moved to :", newPosition)
		}
	}

	byteSlice := make([]byte, len)
	bytesRead, err := sdb.BlockFile.Read(byteSlice)
	if err != nil {
		if sdb.Debug > 30+2 {
			fmt.Print("ReadBlock ")
			fmt.Println(err)
		}
		os.Exit(-1)
		//log.Fatal(err)
	}
	if sdb.Debug > 3 {
		fmt.Printf("Number of bytes read: %d\n", bytesRead)
		fmt.Printf("Data read: %s\n", byteSlice)
	}
	return byteSlice
}

func (sdb *SmallDB) ReadBlocks(pos int64) ([]BlockStruct, error) {
	res := []BlockStruct{}
	pos_n := pos
	for i := 0; i < (int)(sdb.Config.BlockSize); i++ {
		bab := sdb.ReadBlock(pos_n, BlockStructLen)
		bs, _, err := ToBlock(bab)
		if err != nil {
			return []BlockStruct{}, err
		}
		pos_n = pos_n + BlockStructLen
		res = append(res, bs)
	}
	return res, nil
}

func (sdb *SmallDB) WriteBlocks(pos int64, bsa []BlockStruct) error {
	pos_n := pos
	for i := 0; i < (int)(sdb.Config.BlockSize); i++ {
		bs := bsa[i]
		bab, _, err := FromBlock(bs)
		if err != nil {
			return err
		}
		sdb.WriteBlock(pos_n, bab)
		pos_n = pos_n + BlockStructLen
	}
	return nil
}

func (sdb *SmallDB) CreateDB(fields []string, path string) error {
	if sdb.Debug > 3 {
		fmt.Println("CreateDB begin")
	}
	if sdb.Inited {
		return errors.New("found created db") // база уже создана, для пересоздания надо СБРОСИТЬ это флаг
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// path not exists. check last path element
		pl := strings.Split(path, "/")
		if len(pl) > 1 {
			pn := strings.Join(pl[:len(pl)-1], "/")
			if _, err := os.Stat(pn); os.IsNotExist(err) {
				return err
			}
			err := os.MkdirAll(pl[len(pl)-1], 0777)
			if err == nil || os.IsExist(err) {
				// return -101 //warum?
			} else {
				return err // What's going not right
			}
		} else {
			// this is name. create directory
			err := os.MkdirAll(path, 0777)
			if err == nil || os.IsExist(err) {
				return err //warum?
			} else {
				return err // What's going not right
			}
		}
	}
	var field_qty int32
	// удаляем старые файлы если они есть
	err := os.Remove(path + "/" + sdb.Config.BlocksFileName)
	if err != nil {
		fmt.Println(err)
		// skip error
	}
	err = os.Remove(path + "/" + sdb.Config.DataFileName)
	if err != nil {
		fmt.Println(err)
		// skip error
	}
	for _, it := range sdb.Config.IndexFilesName {
		err = os.Remove(path + "/" + it)
		if err != nil {
			fmt.Println(err)
			// skip error
		}
	}
	err = os.Remove(path + "/" + "config.json")
	if err != nil {
		fmt.Println(err)
		// skip error
	}
	// анализируем поля
	field_qty = (int32)(len(fields))
	if field_qty == 0 {
		return errors.New("empty fields list")
	}
	if field_qty > 32 {
		return errors.New("fields list great 32")
	}
	sdb.Config.FieldsName = []string{}
	r, _ := regexp.Compile(`^([a-z]|[а-я]|[0-9]|[_]){1,64}$`)
	for _, it := range fields {
		// надо проверять на недопустимые символы
		str := strings.ToLower(it)
		matched := r.MatchString(str)
		if matched {
			sdb.Config.FieldsName = append(sdb.Config.FieldsName, it)
		} else {
			return fmt.Errorf("bad field name %v", it)
		}
	}
	// открываем файл данных и записываем заголовок
	sdb.OpenData()
	dhs := DataHeaderStruct{}
	dhs.Id = 1
	dhs.Cnt = 0
	dhs.Field_qty = field_qty
	ba, _, err := FromDataHeader(dhs)
	if err != nil {
		return err // ошибка
	}
	sdb.WriteData(-1, ba)
	// формируем файл с пустым блоком на все количество записей
	sdb.OpenBlock()
	bhs := BlockHeaderStruct{}
	bhs.Id = 0
	bhs.PointerPrevBlock = 0
	bhs.PointerNextBlock = 0
	ba1, _, err := FromBlockHeader(bhs)
	if err != nil {
		return err
	}
	sdb.WriteBlock(-1, ba1)
	// создаем индекс записей
	sdb.OpenIndex(RowIndexID)
	ihs := IndexHeaderStruct{}
	ihs.Id = RowIndexID
	ihs.Mask = 0
	ihs.IsFree = 0
	ba2, _, err := FromIndexHeader(ihs)
	if err != nil {
		return err
	}
	sdb.WriteIndex(RowIndexID, -1, ba2)

	is := IndexStruct{}
	is.Number = 0
	is.PointerFar = 0
	is.PointerNear = 0
	is.State = 0 // индексная запись пустая!
	// записываем пустые данные в блок
	ba3, _, err := FromIndex(is)
	if err != nil {
		return err
	}
	var i uint32
	for i = 0; i < sdb.Config.HashTableSize; i++ {
		sdb.WriteIndex(RowIndexID, -1, ba3)
	}
	sdb.CloseIndex(RowIndexID)
	if sdb.Config.UseDeletedData > 0 {
		// создаем файл и индекс для удаляемых записей
		// открываем файл данных и записываем заголовок
		sdb.OpenDeletedData()
		dhs := DataHeaderStruct{}
		dhs.Id = 1
		dhs.Cnt = 0
		dhs.Field_qty = field_qty
		ba, _, err := FromDataHeader(dhs)
		if err != nil {
			return err
		}
		sdb.WriteDeletedData(-1, ba)

		// создаем индекс записей
		sdb.OpenDeletedDataIndex()
		ihs := IndexHeaderStruct{}
		ihs.Id = RowIndexID
		ihs.Mask = 0
		ihs.IsFree = 0
		ba2, _, err := FromIndexHeader(ihs)
		if err != nil {
			return err
		}
		sdb.WriteDeletedDataIndex(-1, ba2)

		is := IndexStruct{}
		is.Number = 0
		is.PointerFar = 0
		is.PointerNear = 0
		is.State = 0 // индексная запись пустая!
		// записываем пустые данные в блок
		ba3, _, err := FromIndex(is)
		if err != nil {
			return err
		}
		var i uint32
		for i = 0; i < sdb.Config.HashTableSize; i++ {
			sdb.WriteDeletedDataIndex(-1, ba3)
		}
		sdb.CloseDeletedDataIndex()
		sdb.CloseDeletedData()
	}
	if sdb.Config.UseJournal > 0 {
		// создаем файл журнала
		sdb.OpenJournal()
	}
	sdb.CloseData()
	sdb.CloseBlock()
	sdb.StoreConfigSmallDB()
	if sdb.Debug > 3 {
		fmt.Printf("Store_Config_SmallDB done\r\n")
	}
	sdb.Inited = true
	return nil
}

func (sdb *SmallDB) Hash(key string) uint32 {
	var h uint32 = 0

	for _, p := range []byte(key) {
		h = h*HashTabMul + (uint32)(p)
	}
	return h % (uint32)(sdb.Config.HashTableSize)
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
	sdb.Config.IndexFilesName = append(sdb.Config.IndexFilesName, "index"+fmt.Sprintf("%x", index_mask)+".bin")
	num := len(sdb.Config.IndexFilesName) - 1
	sdb.OpenIndex(num)
	ihs := IndexHeaderStruct{}
	ihs.Id = 0
	ihs.Mask = index_mask
	ihs.IsFree = 0
	ba1, _, err := FromIndexHeader(ihs)
	if err != nil {
		return err
	}
	sdb.WriteIndex(num, -1, ba1)

	is := IndexStruct{}
	is.Number = 0
	is.PointerFar = 0
	is.PointerNear = 0
	is.State = 0 // индексная запись пустая!
	// записываем пустые данные в блок
	ba2, _, err := FromIndex(is)
	if err != nil {
		return err
	}
	var i uint32
	for i = 0; i < sdb.Config.HashTableSize; i++ {
		sdb.WriteIndex(num, -1, ba2)
	}

	ic := IndexConfig{fields, false, index_mask}
	sdb.Config.Indexes = append(sdb.Config.Indexes, ic)

	inx := strings.Join(fields, ",")

	sdb.Config.IndexesMap[inx] = ic

	if false {
		sdb.OpenFreeIndexData(num)
		fidhs := FreeIndexDataHeaderStruct{}
		fidhs.Id = 1
		fidhs.Cnt = 0
		ba, _, err := FromFreeIndexDataHeader(fidhs)
		if err != nil {
			return err
		}
		sdb.WriteFreeIndexData(num, -1, ba)
		sdb.CloseFreeIndexData(num)
	}

	sdb.CloseIndex(num)
	sdb.StoreConfigSmallDB()
	return nil
}

func uint64Hasher(algorithm hash.Hash64, text string) int64 {
	algorithm.Write([]byte(text))
	return int64(algorithm.Sum64())
}

func (sdb *SmallDB) CreateIndexFree(index_name string) error {
	sdb.Config.IndexFilesName = append(sdb.Config.IndexFilesName, "index_"+index_name+".bin")

	num_free := 0
	for i := range sdb.Config.Indexes {
		ic := sdb.Config.Indexes[i]
		if ic.Free {
			num_free = num_free + 1
		}
	}

	num := len(sdb.Config.IndexFilesName) + num_free - 1

	sdb.OpenIndex(num)
	ihs := IndexHeaderStruct{}
	ihs.Id = 0
	ihs.IsFree = 1
	algorithm := fnv.New64a()
	ihs.Mask = uint64Hasher(algorithm, index_name)

	ba1, _, err := FromIndexHeader(ihs)
	if err != nil {
		return err
	}
	sdb.WriteIndex(num, -1, ba1)

	is := IndexStruct{}
	is.Number = 0
	is.PointerFar = 0
	is.PointerNear = 0
	is.State = 0 // индексная запись пустая!
	// записываем пустые данные в блок
	ba2, _, err := FromIndex(is)
	if err != nil {
		return err
	}
	var i uint32
	for i = 0; i < sdb.Config.HashTableSize; i++ {
		sdb.WriteIndex(num, -1, ba2)
	}

	sdb.OpenFreeIndexData(num)
	fidhs := FreeIndexDataHeaderStruct{}
	fidhs.Id = 1
	fidhs.Cnt = 0
	ba, _, err := FromFreeIndexDataHeader(fidhs)
	if err != nil {
		return err
	}
	sdb.WriteFreeIndexData(num, -1, ba)
	sdb.CloseFreeIndexData(num)

	ic := IndexConfig{[]string{index_name}, true, int64(num)}
	sdb.Config.Indexes = append(sdb.Config.Indexes, ic)

	sdb.Config.IndexesMap[index_name] = ic

	sdb.CloseIndex(num)
	sdb.StoreConfigSmallDB()
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
		strL := strings.Split(str, ",")
		n := 0
		for k, it := range strL {
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
			strL := reflect.ValueOf(object.Index(k).Interface())
			it := ""
			switch strL.Kind() {
			case reflect.String:
				it = strL.String()
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

func (sdb *SmallDB) OpenDB() error {
	if !sdb.Inited {
		return errors.New("not initialized db")
	}
	// открываем файл данных и считываем заголовок
	sdb.OpenData()
	lenDataHeader := DataHeaderStructLen
	ba, err11 := sdb.ReadData(0, lenDataHeader)
	if err11 != nil {
		fmt.Printf("Error %v\r\n", err11)
		return err11
	}
	dhs, err, err1 := ToDataHeader(ba)
	if err < 0 {
		fmt.Printf("Error %v %v\r\n", err, err1)
		return err1
	}
	sdb.Dhs = dhs
	// формируем индексный файл на все количество записей
	sdb.OpenBlock()
	lenBlockHeader := BlockHeaderStructLen
	bab := sdb.ReadBlock(0, lenBlockHeader)
	bhs, err, err1 := ToBlockHeader(bab)
	if err < 0 {
		fmt.Printf("Error %v %v\r\n", err, err1)
		return err1
	}
	sdb.Bhs = bhs
	// теоретически надо загрузить начальный блок?
	// открываем известные индексы
	for i := range sdb.Config.IndexFilesName {
		sdb.OpenIndex(i)
		lenIndexHeader := IndexHeaderStructLen
		bai := sdb.ReadIndex(i, 0, lenIndexHeader)
		ihs, err, err1 := ToIndexHeader(bai)
		if err < 0 {
			fmt.Printf("Error %v %v\r\n", err, err1)
			return err1
		}
		sdb.IhsA = append(sdb.IhsA, ihs)
	}
	// загружаем индекс
	sdb.OpenIndex(RowIndexID)
	lenIndexHeader := IndexHeaderStructLen
	bai := sdb.ReadIndex(RowIndexID, 0, lenIndexHeader)
	ihs, err, err1 := ToIndexHeader(bai)
	if err < 0 {
		fmt.Printf("Error %v %v\r\n", err, err1)
		return err1
	}
	sdb.RIIhs = ihs
	sdb.FreeIndexDataFiles = make(map[int]*os.File)

	if sdb.Config.UseDeletedData > 0 {
		// создаем файл и индекс для удаляемых записей
		// открываем файл данных и записываем заголовок
		sdb.OpenDeletedData()
		lenDataHeader := DataHeaderStructLen
		ba := sdb.ReadDeletedData(0, lenDataHeader)
		dhs, err, err1 := ToDataHeader(ba)
		if err < 0 {
			fmt.Printf("Error %v %v\r\n", err, err1)
			return err1
		}
		sdb.DDhs = dhs
		sdb.OpenDeletedDataIndex()
		lenIndexHeader := IndexHeaderStructLen
		bai := sdb.ReadDeletedDataIndex(0, lenIndexHeader)
		ihs, err, err1 := ToIndexHeader(bai)
		if err < 0 {
			fmt.Printf("Error %v %v\r\n", err, err1)
			return err1
		}
		sdb.DDIhs = ihs
	}
	if sdb.Config.UseJournal > 0 {
		// создаем файл журнала
		sdb.OpenJournal()
	}
	// открываем доступ к базе
	sdb.Opened = true
	return nil
}

type IndexData struct {
	Data string
	Mask int64
	Pos  int64
}

func (sdb *SmallDB) MakeIndexData(ind int, inxd IndexData, pos_g int64) error {
	inx := sdb.Hash(inxd.Data)
	posInx := (int64)(IndexHeaderStructLen + inx*IndexStructLen)
	bai := sdb.ReadIndex(ind, posInx, IndexStructLen)
	is, _, err := ToIndex(bai)
	if err != nil {
		return err
	}
	// проверяем состояние индексной записи, если == 0 то пустая
	if is.State == 0 {
		// этого блока нет
		is.Number = (int64)(inx)
		// строим новый блок
		// первый из группы
		bs := BlockStruct{}
		bs.Id = (int64)(inx)
		bs.PointerData = inxd.Pos
		bs.PointerFar = 0
		bs.PointerNear = 0
		ba2, _, err := FromBlock(bs)
		if err != nil {
			return err
		}
		pos := sdb.WriteBlock(-1, ba2)
		// остальные из группы размер в sdb.Config.Block_size
		// записываем пустые данные в блок
		bs.Id = 0
		bs.PointerData = 0
		ba3, _, err := FromBlock(bs)
		if err != nil {
			return err
		}
		for i := 1; i < (int)(sdb.Config.BlockSize); i++ {
			sdb.WriteBlock(-1, ba3)
		}
		is.PointerFar = pos
		is.PointerNear = 0
		is.State = IndexUsed
		bai, _, err = FromIndex(is)
		if err != nil {
			return err
		}
		sdb.WriteIndex(ind, posInx, bai)
	} else {
		nextPtr := is.PointerFar
		firstPtr := nextPtr
		flagBreak := false
		var bsFirst BlockStruct
		flagUse := true
		for {
			// такой блок уже есть считываем его
			// читаем группу блоков из из sdb.Config.Block_size
			bsa, err := sdb.ReadBlocks((int64)(nextPtr))
			if err != nil {
				return err
			}
			if nextPtr == firstPtr {
				bsFirst = bsa[0]
				// смотрим, что в первом блоке - .PointerFar ненулевое значение.
				// переходим сразу к этому номеру блока
				if bsa[sdb.Config.BlockSize-1].PointerFar != 0 {
					nextPtr = bsa[sdb.Config.BlockSize-1].PointerFar
					flagUse = false
				}
			} else {
				flagUse = true
			}
			if flagUse {
				// ищем конец и добавляем
				flag := true
				for j, bs := range bsa {
					if bs.PointerData == 0 {
						bsa[j].Id = (int64)(inx)
						bsa[j].PointerData = inxd.Pos
						bsa[j].PointerFar = 0
						bsa[j].PointerNear = 0
						if j > 0 {
							bsa[j-1].PointerNear = (int32)(j * BlockStructLen)
						}
						sdb.WriteBlocks((int64)(nextPtr), bsa)
						flag = false
						flagBreak = true
						break
					}
				}
				if flag {
					// проверяем, что в последнем блоке нет указателя
					if bsa[sdb.Config.BlockSize-1].PointerFar != 0 {
						nextPtr = bsa[sdb.Config.BlockSize-1].PointerFar
					} else {
						// строим новый блок
						bs := BlockStruct{}
						bs.Id = (int64)(inx)
						bs.PointerData = inxd.Pos
						bs.PointerFar = 0
						bs.PointerNear = 0
						ba2, _, err := FromBlock(bs)
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
						ba3, _, err := FromBlock(bs)
						if err != nil {
							return err
						}
						// записываем остальные пустые блоки
						for k := 1; k < (int)(sdb.Config.BlockSize); k++ {
							sdb.WriteBlock(-1, ba3)
						}
						// корректируем в последнем блоке дальний указатель на позицию нового блока
						bsa[sdb.Config.BlockSize-1].PointerFar = pos
						bsa[sdb.Config.BlockSize-1].PointerNear = 0
						sdb.WriteBlocks((int64)(nextPtr), bsa)
						// и корректируем PointerFar в первом элементе самого первого блока
						bsFirst.PointerFar = pos
						ba4, _, err := FromBlock(bsFirst)
						if err != nil {
							return err
						}
						sdb.WriteBlock(firstPtr, ba4)
						flagBreak = true
					}
				}
			}
			if flagBreak {
				break
			}
		}
	}
	return nil
}

func (sdb *SmallDB) StoreRecordOnMap(args map[string]string) (int64, int64, error) {
	//fmt.Printf("len(args) %v args %#v\r\n", len(args), args)
	argsList := make([]string, len(sdb.FieldsNameMap))
	for k, v := range args {
		fn, ok := sdb.FieldsNameMap[k]
		if !ok {
			fmt.Printf("k %v\r\n", k)
			return 0, -1000, nil
		}
		argsList[fn] = v
	}
	return sdb.StoreRecordStrings(argsList)
}

func (sdb *SmallDB) StoreRecord(args ...string) (int64, int64, error) {
	args_list := []string{}
	args_list = append(args_list, args...)
	return sdb.StoreRecordStrings(args_list)
}

func (sdb *SmallDB) StoreRecordStrings(args []string) (int64, int64, error) {
	// возвращает либо отрицательное значение - ошибка, либо позицию записанных данных
	var result int64 = 0
	var num int64 = -1
	if !sdb.Inited {
		return -1, num, errors.New("data base not inited")
	}
	if sdb.Opened {
		// открываем файл данных и считываем заголовок
		if len(args) == (int)(sdb.Dhs.Field_qty) {
			// формируем запись и параллельно индекс
			inxData := make([]IndexData, len(sdb.IhsA))
			/*
				if false {
					for _ = range sdb.IhsA {
						d := IndexData{}
						d.Data = ""
						d.Pos = 0
						inxData = append(inxData, d)
					}
				}
			*/
			var posG int64 = 0
			if false {
				rowID := uuid.NewV4().Bytes()
				ds := DataStruct{}
				ds.Id = sdb.Cnt
				ds.State = 0
				ds.Field = -1 // RowID
				ds.DataLen = (int32)(len(rowID))
				ba, _, err := FromData(ds)
				if err != nil {
					return -1, -1, err
				}
				pos := sdb.WriteData(-1, ba)
				posG = pos
				// присваиваем значение начала записи
				result = pos
				ba = []byte(rowID)
				sdb.WriteData(-1, ba)
			}
			for i, it := range args {
				ds := DataStruct{}
				ds.Id = sdb.Cnt
				ds.State = 0
				ds.Field = (int32)(i)
				// whats do if length of data is zero?
				ds.DataLen = (int32)(len(it))
				ba, _, err := FromData(ds)
				if err != nil {
					return -1, -1, err
				}
				pos := sdb.WriteData(-1, ba)
				if true {
					if i == 0 {
						posG = pos
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
						inxData[j].Data = inxData[j].Data + " | " + it
						inxData[j].Pos = posG
						inxData[j].Mask = ihs.Mask
					}
				}
			}
			// добавление в RowIndex
			RowIndexData := fmt.Sprintf("%0d", sdb.Cnt)
			inxDataRow := IndexData{}
			inxDataRow.Data = RowIndexData
			inxDataRow.Pos = posG
			sdb.MakeIndexData(RowIndexID, inxDataRow, posG)
			// добавление в остальные индексы
			for i := range inxData {
				inxd := inxData[i]
				if len(inxd.Data) > 0 {
					sdb.MakeIndexData(i, inxd, posG)
				}
			}
			num = sdb.Cnt
			sdb.Cnt = sdb.Cnt + 1
			// сохраняем счетчик записей
			sdb.Dhs.Cnt = sdb.Cnt
			ba, _, err := FromDataHeader(sdb.Dhs)
			if err != nil {
				return -1, -1, err
			}
			sdb.WriteData(0, ba)
		}
	} else {
		return -1, num, errors.New("data base not opened")
	}
	return result, num, nil
}

func (sdb *SmallDB) StoreFreeIndex(indexName string, indexData string, posG int64) (int64, error) {
	result := int64(0)
	if !sdb.Inited {
		return -1, errors.New("data base not inited")
	}
	if sdb.Opened {
		// открываем файл данных и считываем заголовок
		// формируем запись и параллельно индекс
		inxData := make([]IndexData, len(sdb.IhsA))
		/*
			for _, _ = range sdb.IhsA {
				d := IndexData{}
				d.Data = ""
				d.Pos = 0
				inx_data = append(inx_data, d)
			}
		*/
		// найдем имя свободного индекса
		algorithm := fnv.New64a()
		ihsMask := uint64Hasher(algorithm, indexName)
		// надо найти индекс
		for j, ihs := range sdb.IhsA {
			if ihs.IsFree != 0 {
				if ihs.Mask == ihsMask {
					inxData[j].Data = indexData
					inxData[j].Pos = posG
					inxData[j].Mask = ihs.Mask
				}
			}
		}
		for i := range sdb.IhsA {
			inxd := inxData[i]
			if len(inxd.Data) > 0 {
				sdb.MakeIndexData(i, inxd, posG)
			}
		}
		// добавляем запись в свободный индекс
		result = sdb.Cnt
	} else {
		return -5, errors.New("data base not opened")
	}
	return result, nil
}

func (sdb *SmallDB) GetFieldValueByName(rec *common.Record, fieldName string) (string, error) {
	//fmt.Printf("Get_field_value_by_name %#v %v\r\n", rec, field_name)
	fn, ok := sdb.FieldsNameMap[fieldName]
	if !ok {
		return "", fmt.Errorf("bad field name %v", fieldName)
	}
	return rec.FieldsValue[fn], nil
}

func (sdb *SmallDB) GetFieldsValueWithName(rec *common.Record) ([][]string, error) {
	//fmt.Printf("Get_field_value_by_name %#v %v\r\n", rec, field_name)
	result := [][]string{}
	if len(rec.FieldsValue) != len(sdb.FieldsNameMap) {
		return result, errors.New("number of fields in record not equal number of fields in database")
	}
	for k, v := range sdb.FieldsNameMap {
		result = append(result, []string{k, rec.FieldsValue[v]})
	}
	return result, nil
}

func (sdb *SmallDB) FindRecord(ind int, args ...string) ([]*common.Record, int, error) {
	args_list := []string{}
	args_list = append(args_list, args...)
	return sdb.FindRecordStringArray(ind, args_list)
}

func (sdb *SmallDB) FindRecordIndexString(index []string, args []string) ([]*common.Record, int, error) {
	ind := sdb.GetIndexIdByStringList(index)
	if sdb.Debug > 1 {
		fmt.Printf("ind %v\r\n", ind)
	}

	return sdb.FindRecordStringArray(int(ind), args)
}

func (sdb *SmallDB) FindRecordStringArray(ind int, args []string) ([]*common.Record, int, error) {
	dataRes := []*common.Record{}
	if !sdb.Inited {
		return dataRes, -1, errors.New("data base not inited")
	}
	if !sdb.Opened {
		return dataRes, -5, errors.New("data base not opened")
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
		posInx := (int64)(IndexHeaderStructLen + inx*IndexStructLen)
		if sdb.Debug > 3 {
			fmt.Printf("inx %v read pos_inx %x \r\n", inx, posInx)
		}
		bai := sdb.ReadIndex(ind, posInx, IndexStructLen)
		is, _, err := ToIndex(bai)
		if err != nil {
			return dataRes, -10, err
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
					return dataRes, -11, err
				}
				// ищем конец и добавляем
				flagEndBlock := false
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
							len_header := DataStructLen
							ba, err11 := sdb.ReadData(ptr, len_header)
							if err11 != nil {
								// fmt.Printf("Error %v\r\n", err11)
								return dataRes, -12, err11
							}
							ds, err, err1 := ToData(ba)
							if err < 0 {
								// fmt.Printf("Error %v %v\r\n", err, err1)
								return dataRes, -13, err1
							}
							ptr = ptr + (int64)(len_header)
							d := ""
							if ds.DataLen == 0 {
								// return nil, -7, nil
								data = append(data, "")
							} else {
								ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
								if err11 != nil {
									// fmt.Printf("Error %v\r\n", err11)
									return dataRes, -14, err11
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
							rec := common.Record{Num: num, FieldsValue: data}
							// добавляем в выборку
							dataRes = append(dataRes, &rec)
							// зачем прекращать выборку????
						}
					} else {
						// блок похоже без данных!
						flagEndBlock = true
					}
				}
				if flagEndBlock {
					break
				} else {
					next_ptr = bsa[sdb.Config.BlockSize-1].PointerFar
					if next_ptr == 0 {
						break
					}
				}
			}
			return dataRes, 0, nil
		}
	}
	return nil, -2, errors.New("no data")
}

func (sdb *SmallDB) DeleteRecord(rec int64) (int, error) {
	ind := RowIndexID
	if sdb.Debug > 1 {
		fmt.Printf("ind %v\r\n", ind)
	}
	if !sdb.Inited {
		return -1, errors.New("data base not inited")
	}
	if !sdb.Opened {
		return -5, errors.New("data base not opened")
	}

	dataN := fmt.Sprintf("%v", rec)
	// сформировали, ищем
	inx := sdb.Hash(dataN)
	posInx := (int64)(IndexHeaderStructLen + inx*IndexStructLen)
	if sdb.Debug > 3 {
		fmt.Printf("ind %v read pos_inx %x \r\n", ind, posInx)
	}
	bai := sdb.ReadIndex(ind, posInx, IndexStructLen)
	is, _, err := ToIndex(bai)
	if err != nil {
		return -15, err
	}
	if sdb.Debug > 3 {
		fmt.Printf("is.Number %v is.State %v\r\n", is.Number, is.State)
	}
	// проверяем, что блок используется
	if is.State != 0 {
		nextPtr := is.PointerFar
		// такой блок есть, считываем его
		for {
			if sdb.Debug > 3 {
				fmt.Printf("block next_ptr %x\r\n", nextPtr)
			}
			bsa, err := sdb.ReadBlocks((int64)(nextPtr))
			if err != nil {
				return -16, err
			}
			// ищем конец и добавляем
			flagEndBlock := false
			for j, bs := range bsa {
				// читаем данные и проверяем на соответствие
				if bs.PointerData != 0 {
					flag := true
					ptr := bs.PointerData
					if sdb.Debug > 3 {
						fmt.Printf("data ptr %v j %v\r\n", ptr, j)
					}
					for i := 0; i < (int)(sdb.Dhs.Field_qty); i++ {
						lenHeader := DataStructLen
						ba, err11 := sdb.ReadData(ptr, lenHeader)
						if err11 != nil {
							// fmt.Printf("Error %v\r\n", err11)
							return -12, err11
						}
						ds, err, err1 := ToData(ba)
						if err < 0 {
							// fmt.Printf("Error %v %v\r\n", err, err1)
							return -17, err1
						}
						// меняем и сохранем
						ds.State = 1
						ba, _, err1 = FromData(ds)
						if err1 != nil {
							return -17, err1
						}
						sdb.WriteData(ptr, ba)

						ptr = ptr + (int64)(lenHeader)
						if ds.DataLen == 0 {
							//return 0, nil
						} else {
							ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
							if err11 != nil {
								// fmt.Printf("Error %v\r\n", err11)
								return -13, err11
							}
							d := string(ba)
							if sdb.Debug > 3 {
								fmt.Printf("ds %v d %v ptr %v\r\n", ds, d, ptr)
							}
						}
						ptr = ptr + (int64)(ds.DataLen)
					}
					if flag {
						// помечаем блок
						bsa[j].PointerData = 0
						flagEndBlock = true
					}
				} else {
					flagEndBlock = true
				}
			}

			if flagEndBlock {
				sdb.WriteBlocks((int64)(is.PointerFar), bsa)
				break
			} else {
				nextPtr = bsa[sdb.Config.BlockSize-1].PointerFar
				if nextPtr == 0 {
					break
				}
			}
		}
		return 0, nil
	}
	return -2, errors.New("no data")
}

func (sdb *SmallDB) LoadRecords(rec int) ([]*common.Record, int, error) {
	data := []*common.Record{}
	if !sdb.Inited {
		return data, -1, errors.New("data base not inited")
	}
	if sdb.Opened {
		ptr := (int64)(DataHeaderStructLen)
		// открываем файл данных и считываем по очереди
		for j := 0; j < rec; j++ {
			if sdb.Debug > 5 {
				fmt.Printf("current rec %v\r\n", j)
			}
			var i int32
			var num int64
			dataR := []string{}
			if sdb.Debug > 6 {
				fmt.Printf("sdb.Dhs.Field_qty %v\r\n", sdb.Dhs.Field_qty)
			}
			for i = 0; i < sdb.Dhs.Field_qty; i++ {
				lenHeader := DataStructLen
				ba, err11 := sdb.ReadData(ptr, lenHeader)
				if err11 != nil {
					// fmt.Printf("Error %v\r\n", err11)
					return data, -12, err11
				}
				if sdb.Debug > 9 {
					fmt.Printf("ba %v\r\n", ba)
				}

				ds, err1, err := ToData(ba)
				if err != nil {
					fmt.Printf("Error %v %v\r\n", err, err1)
					return data, -13, err
				}

				if sdb.Debug > 7 {
					fmt.Printf("ds %#v\r\n", ds)
				}

				ptr = ptr + (int64)(lenHeader)
				if ds.State == 0 {
					if ds.DataLen == 0 {
						// it is not error - just no data
						dataR = append(dataR, "")
					} else {
						ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
						if err11 != nil {
							// fmt.Printf("Error %v\r\n", err11)
							return data, -13, err11
						}
						if sdb.Debug > 9 {
							fmt.Printf("ds ba %v\r\n", ba)
						}

						d := string(ba)
						dataR = append(dataR, d)
					}
					num = ds.Id
				}
				ptr = ptr + (int64)(ds.DataLen)
			}
			if len(dataR) > 0 {
				rec := common.Record{Num: num, FieldsValue: dataR}
				data = append(data, &rec)
				if sdb.Debug > 5 {
					fmt.Printf("data %v\r\n", data)
				}
			}
		}
	} else {
		return data, -5, errors.New("data base not opened")
	}
	return data, 0, nil
}

func (sdb *SmallDB) LoadRecord(rec int64) ([]*common.Record, int, error) {
	data := []*common.Record{}
	ind := RowIndexID
	if sdb.Debug > 1 {
		fmt.Printf("ind %v\r\n", ind)
	}
	if !sdb.Inited {
		return data, -1, errors.New("data base not inited")
	}
	if !sdb.Opened {
		return data, -5, errors.New("data base not opened")
	}

	dataN := fmt.Sprintf("%v", rec)
	// сформировали, ищем
	inx := sdb.Hash(dataN)
	posInx := (int64)(IndexHeaderStructLen + inx*IndexStructLen)
	if sdb.Debug > 3 {
		fmt.Printf("ind %v read pos_inx %x \r\n", ind, posInx)
	}
	bai := sdb.ReadIndex(ind, posInx, IndexStructLen)
	is, _, err := ToIndex(bai)
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
			flagEndBlock := false
			for j, bs := range bsa {
				// читаем данные и проверяем на соответствие
				if bs.PointerData != 0 {
					//flag := true
					ptr := bs.PointerData
					var num int64
					dataR := []string{}
					if sdb.Debug > 3 {
						fmt.Printf("data ptr %v j %v\r\n", ptr, j)
					}
					for i := 0; i < (int)(sdb.Dhs.Field_qty); i++ {
						lenHeader := DataStructLen
						ba, err11 := sdb.ReadData(ptr, lenHeader)
						if err11 != nil {
							// fmt.Printf("Error %v\r\n", err11)
							return data, -12, err11
						}
						ds, err, err1 := ToData(ba)
						if err < 0 {
							// fmt.Printf("Error %v %v\r\n", err, err1)
							return data, -17, err1
						}
						if sdb.Debug > 7 {
							fmt.Printf("ds %#v\r\n", ds)
						}
						ptr = ptr + (int64)(lenHeader)
						if ds.State == 0 {
							if ds.DataLen == 0 {
								// it is not error - just no data
								dataR = append(dataR, "")
							} else {
								ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
								if err11 != nil {
									// fmt.Printf("Error %v\r\n", err11)
									return data, -13, err11
								}
								if sdb.Debug > 9 {
									fmt.Printf("ds ba %v\r\n", ba)
								}

								d := string(ba)
								dataR = append(dataR, d)
							}
							num = ds.Id
						}
						ptr = ptr + (int64)(ds.DataLen)
					}
					if num == rec {
						if len(dataR) > 0 {
							rec := common.Record{Num: num, FieldsValue: dataR}
							data = append(data, &rec)
							if sdb.Debug > 5 {
								fmt.Printf("data %v\r\n", data)
							}
						}
					}
				} else {
					flagEndBlock = true
				}
			}
			if flagEndBlock {
				break
			} else {
				next_ptr = bsa[sdb.Config.BlockSize-1].PointerFar
				if next_ptr == 0 {
					break
				}
			}
		}
		return data, 0, nil
	}
	return data, -2, errors.New("no data")
}

func (sdb *SmallDB) LoadLazyRecords(rec int) (func() (*common.Record, int, error), error) {
	if !sdb.Inited {
		return nil, errors.New("data base not inited")
	}
	if sdb.Opened {
		ptr := (int64)(DataHeaderStructLen)
		// открываем файл данных и считываем по очереди
		j := 0
		lazyLoad := func() (*common.Record, int, error) {
			var data *common.Record
			if sdb.Debug > 5 {
				fmt.Printf("current rec %v\r\n", j)
			}
			var i int32
			var num int64
			dataR := []string{}
			if sdb.Debug > 6 {
				fmt.Printf("sdb.Dhs.Field_qty %v\r\n", sdb.Dhs.Field_qty)
			}
			for i = 0; i < sdb.Dhs.Field_qty; i++ {
				lenHeader := DataStructLen
				ba, err11 := sdb.ReadData(ptr, lenHeader)
				if err11 != nil {
					//fmt.Printf("Error %v\r\n", err11)
					return data, -12, err11
				}
				if sdb.Debug > 9 {
					fmt.Printf("ba %v\r\n", ba)
				}

				ds, err, err1 := ToData(ba)
				if err < 0 {
					fmt.Printf("Error %v %v\r\n", err, err1)
				}
				if sdb.Debug > 7 {
					fmt.Printf("ds %#v\r\n", ds)
				}

				ptr = ptr + (int64)(lenHeader)
				if ds.State == 0 {
					if ds.DataLen == 0 {
						// it is not error - just no data
						// return nil, 0, nil
						dataR = append(dataR, "")
					} else {
						ba, err11 = sdb.ReadData(ptr, (int)(ds.DataLen))
						if err11 != nil {
							//fmt.Printf("Error %v\r\n", err11)
							return data, -13, err11
						}
						if sdb.Debug > 9 {
							fmt.Printf("ds ba %v\r\n", ba)
						}

						d := string(ba)
						dataR = append(dataR, d)
					}
					num = ds.Id
				}
				ptr = ptr + (int64)(ds.DataLen)
			}
			if len(dataR) > 0 {
				rec := common.Record{Num: num, FieldsValue: dataR}
				data = &rec
				if sdb.Debug > 5 {
					fmt.Printf("data %v\r\n", data)
				}
			}
			return data, 0, nil
		}
		return lazyLoad, nil
	}
	return nil, errors.New("data base not opened")
}
