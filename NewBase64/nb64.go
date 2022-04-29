package NewBase64

import (
	"encoding/binary"
	"errors"
)

var SymbolArray = []byte{'A',
	'B',
	'C',
	'D',
	'E',
	'F',
	'G',
	'H',
	'I',
	'J',
	'K',
	'L',
	'M',
	'N',
	'O',
	'P',
	'Q',
	'R',
	'S',
	'T',
	'U',
	'V',
	'W',
	'X',
	'Y',
	'Z',
	'a',
	'b',
	'c',
	'd',
	'e',
	'f',
	'g',
	'h',
	'i',
	'j',
	'k',
	'l',
	'm',
	'n',
	'o',
	'p',
	'q',
	'r',
	's',
	't',
	'u',
	'v',
	'w',
	'x',
	'y',
	'z',
	'0',
	'1',
	'2',
	'3',
	'4',
	'5',
	'6',
	'7',
	'8',
	'9',
	'@',
	'$'}

func SymbolDecode(b byte) (byte, error) {
	var res int = 0
	if (b >= 'A') && (b <= 'Z') {
		res = int(b) - int(byte('A'))
	} else {
		if (b >= 'a') && (b <= 'z') {
			res = 26 + int(b) - int(byte('a'))
		} else {
			if (b >= '0') && (b <= '9') {
				res = 26 + 26 + int(b) - int(byte('0'))
			} else {
				if b == '@' {
					res = 26 + 26 + 10
				} else {
					if b == '$' {
						res = 26 + 26 + 10 + 1
					} else {
						return 0, errors.New("Symbol error")
					}
				}

			}
		}

	}
	return byte(res), nil
}

func BytesEncode(bl []byte) []byte {
	re := []byte{}
	len_bl := len(bl)
	for i := 0; i < len_bl; {
		l := i + 3
		var bb []byte
		if l >= len_bl {
			bb = make([]byte, 4)
			copy(bb, bl[i:len_bl])
		} else {
			bb = make([]byte, 4)
			copy(bb, bl[i:l])
		}
		for j := 0; j < 4; j++ {
			d0 := bb[0] & 0b00111111
			bb[0] = bb[0] & 0b11000000
			re = append(re, SymbolArray[d0])
			data := binary.LittleEndian.Uint32(bb)
			data = data >> 6
			binary.LittleEndian.PutUint32(bb, data)
		}
		i = i + 3
	}
	return re
}

func BytesDecode(bl []byte) ([]byte, error) {
	p := len(bl)
	if p < 4 {
		return []byte{}, errors.New("Bytes array size less 4")
	}

	re := []byte{}
	for i := 0; i < len(bl); {
		var data uint32 = 0
		bb_r := bl[i : i+4]
		len_bb_r := len(bb_r)
		bb := make([]byte, len_bb_r)
		for i, _ := range bb_r {
			b, err := SymbolDecode(bb_r[i])
			if err != nil {
				return []byte{}, err
			}
			bb[len_bb_r-1-i] = b
		}
		bbn := make([]byte, 4)
		for i := 0; i < len(bb); i++ {
			if i > 0 {
				data = data << 6
			}
			bbn[0] = bb[i]
			data_p := binary.LittleEndian.Uint32(bbn)
			data = data | data_p
		}
		binary.LittleEndian.PutUint32(bbn, data)
		bbn = bbn[0:3]
		re = append(re, bbn...)
		i = i + 4
	}
	return re, nil
}
