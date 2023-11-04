package index

import (
	"encoding/json"
	"fmt"
	"os"
)

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
	file, err := os.ReadFile(path + "/" + "config.json")
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
	err1 := os.WriteFile(sdb.Path+"/"+"config.json", ba, 0777)
	if err1 != nil {
		fmt.Println(err1)
		os.Exit(-1)
	}
	return nil
}

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
