package main

import (
	"fmt"
	. "github.com/wanderer69/SmallDB/Expr"
)

func main() {
        fmt.Printf("%#v\r\n", Expression_parse("=10*"))
        fmt.Printf("%#v\r\n", Expression_parse("=*"))
        fmt.Printf("%#v\r\n", Expression_parse("=10"))
        fmt.Printf("%#v\r\n", Expression_parse("=10l256*"))
        fmt.Printf("%#v\r\n", Expression_parse("=l512*"))

        fmt.Printf("%#v\r\n", Expression_parse("=10sёж.*"))
        fmt.Printf("%#v\r\n", Expression_parse("=stest.*"))
        fmt.Printf("%#v\r\n", Expression_parse("=10l256slabel.*"))
        fmt.Printf("%#v\r\n", Expression_parse("=l512sёжик.*"))
        fmt.Printf("%#v\r\n", Expression_parse("="))
        fmt.Printf("%#v\r\n", Expression_parse("10*"))
}
