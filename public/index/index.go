package index

import (
	"fmt"
	//"io/ioutil"

	"bytes"

	"encoding/binary"
)

const HashTabSize = 0x3FFFF
const HashTabMul = 31

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
