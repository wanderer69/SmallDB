package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

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
