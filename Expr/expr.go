package expression

import (
	"fmt"
	"strconv"
	"unicode"
)

type Expr struct {
	ValueI int
	ValueS string
	Step   int
	Len    int
	Mode   string
	Symbol string
}

func Expression_parse(str string) *Expr {
	// fmt.Printf("-> %v\r\n", str)
	k := 0
	res := Expr{}
	if str[k] == '=' {
		// fmt.Printf("%v\r\n", str[k])
		k = k + 1
		// this expression
		s := ""
		for i := k; i < len(str); i++ {
			if unicode.IsDigit(rune(str[i])) {
				s = s + str[i:i+1]
				// fmt.Printf(" s %v\r\n", s)
				k = k + 1
			} else {
				break
			}
		}
		// fmt.Printf("k %v s %v\r\n", k, s)
		num := -1
		if len(s) > 0 {
			num1, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				fmt.Printf("Error %v\r\n", err)
				return nil
			}
			num = int(num1)
		}
		// fmt.Printf("num %v k %v len %v\r\n", num, k, len(str))

		if k == len(str) {
			// number not value
			if num < 0 {
				return nil
			} else {
				res.Mode = "ValueInt"
				res.ValueI = int(num)
				return &res
			}
		}
		// fmt.Printf("k %v str[k] %v\r\n", k, str[k:k+1])

		if str[k] == 'l' {
			k = k + 1
			s = ""
			// len test block
			for i := k; i < len(str); i++ {

				if unicode.IsDigit(rune(str[i])) {
					s = s + str[i:i+1]
					// fmt.Printf(" s %v\r\n", s)
					k = k + 1
				} else {
					break
				}
			}
			// fmt.Printf("->s '%v'\r\n", s)
			if len(s) > 0 {
				num1, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					fmt.Printf("Error %v\r\n", err)
					return nil
				}
				res.Len = int(num1)
			}
		}

		if str[k] == 's' {
			k = k + 1
			s = ""
			// len test block
			for i := k; i < len(str); i++ {
				if rune(str[i]) != '.' {
					s = s + str[i:i+1]
					// fmt.Printf(" s %v\r\n", s)
					k = k + 1
				} else {
					k = k + 1
					break
				}
			}
			//fmt.Printf("->s '%v' %c\r\n", s, str[k])
			if len(s) > 0 {
				res.Symbol = s
			}
		}

		if str[k] == '!' {
			// field value from file 
			res.Mode = "FromFile"
			return &res
		}

		if str[k] == '*' {
			if num < 0 {
				res.Mode = "Random"
				return &res
			} else {
				res.Mode = "RandomStep"
				res.Step = int(num)
				return &res
			}
		}
		return nil
	}
	return nil
}

func Test_main() {
	fmt.Printf("%#v\r\n", Expression_parse("=10*"))
	fmt.Printf("%#v\r\n", Expression_parse("=*"))
	fmt.Printf("%#v\r\n", Expression_parse("=10"))
	fmt.Printf("%#v\r\n", Expression_parse("="))
	fmt.Printf("%#v\r\n", Expression_parse("10*"))
	fmt.Printf("%#v\r\n", Expression_parse("=10l256*"))
	fmt.Printf("%#v\r\n", Expression_parse("=l256*"))
}
