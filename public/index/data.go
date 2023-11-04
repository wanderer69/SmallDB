package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

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
