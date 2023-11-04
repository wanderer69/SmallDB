package expression

import (
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

func ExpressionParse(str string) (*Expr, error) {
	k := 0
	res := Expr{}
	if str[k] == '=' {
		k = k + 1
		// this expression
		s := ""
		for i := k; i < len(str); i++ {
			if unicode.IsDigit(rune(str[i])) {
				s = s + str[i:i+1]
				k = k + 1
			} else {
				break
			}
		}
		num := -1
		if len(s) > 0 {
			num1, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, err
			}
			num = int(num1)
		}

		if k == len(str) {
			// number not value
			if num < 0 {
				return nil, nil
			} else {
				res.Mode = "ValueInt"
				res.ValueI = int(num)
				return &res, nil
			}
		}

		if str[k] == 'l' {
			k = k + 1
			s = ""
			// len test block
			for i := k; i < len(str); i++ {

				if unicode.IsDigit(rune(str[i])) {
					s = s + str[i:i+1]
					k = k + 1
				} else {
					break
				}
			}
			if len(s) > 0 {
				num1, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return nil, err
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
					k = k + 1
				} else {
					k = k + 1
					break
				}
			}
			if len(s) > 0 {
				res.Symbol = s
			}
		}

		if str[k] == '!' {
			// field value from file
			res.Mode = "FromFile"
			return &res, nil
		}

		if str[k] == '*' {
			if num < 0 {
				res.Mode = "Random"
				return &res, nil
			} else {
				res.Mode = "RandomStep"
				res.Step = int(num)
				return &res, nil
			}
		}
		return nil, nil
	}
	return nil, nil
}
