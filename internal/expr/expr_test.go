package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpression(t *testing.T) {

	expression := &Expr{ValueI: 0, ValueS: "", Step: 10, Len: 0, Mode: "RandomStep", Symbol: ""}
	require.Equal(t, expression, Expression_parse("=10*"))
	expression = &Expr{ValueI: 0, ValueS: "", Step: 0, Len: 0, Mode: "Random", Symbol: ""}
	require.Equal(t, expression, Expression_parse("=*"))
	expression = &Expr{ValueI: 10, ValueS: "", Step: 0, Len: 0, Mode: "ValueInt", Symbol: ""}
	require.Equal(t, expression, Expression_parse("=10"))
	expression = &Expr{ValueI: 0, ValueS: "", Step: 10, Len: 256, Mode: "RandomStep", Symbol: ""}
	require.Equal(t, expression, Expression_parse("=10l256*"))
	expression = &Expr{ValueI: 0, ValueS: "", Step: 0, Len: 512, Mode: "Random", Symbol: ""}
	require.Equal(t, expression, Expression_parse("=l512*"))

	expression = &Expr{ValueI: 0, ValueS: "", Step: 10, Len: 0, Mode: "RandomStep", Symbol: "ёж"}
	require.Equal(t, expression, Expression_parse("=10sёж.*"))
	expression = &Expr{ValueI: 0, ValueS: "", Step: 0, Len: 0, Mode: "Random", Symbol: "test"}
	require.Equal(t, expression, Expression_parse("=stest.*"))
	expression = &Expr{ValueI: 0, ValueS: "", Step: 10, Len: 256, Mode: "RandomStep", Symbol: "label"}
	require.Equal(t, expression, Expression_parse("=10l256slabel.*"))
	expression = &Expr{ValueI: 0, ValueS: "", Step: 0, Len: 512, Mode: "Random", Symbol: "ёжик"}
	require.Equal(t, expression, Expression_parse("=l512sёжик.*"))
	expression = nil
	require.Equal(t, expression, Expression_parse("="))
	expression = nil
	require.Equal(t, expression, Expression_parse("10*"))
}
