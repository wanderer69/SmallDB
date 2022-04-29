package main

import (
	. "arkhangelskiy-dv.ru/SmallDB/NewBase64"
	"fmt"
)

func main() {

	//bi := []byte{0x1f, 0xff, 0xff, 0xff}
	bi := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	if true {
		fmt.Printf("bi %x\r\n", bi)
		bo := BytesEncode(bi)
		fmt.Printf("%v\r\n", string(bo))
		bn, err := BytesDecode(bo)
		if err != nil {
			fmt.Printf("Error %v\r\n", err)
			return
		}
		fmt.Printf("%x\r\n", bn)
	}
}
