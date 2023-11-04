package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpression(t *testing.T) {

	expression := &Expr{ValueI: 0, ValueS: "", Step: 10, Len: 0, Mode: "RandomStep", Symbol: ""}
	result, err := ExpressionParse("=10*")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = &Expr{ValueI: 0, ValueS: "", Step: 0, Len: 0, Mode: "Random", Symbol: ""}
	result, err = ExpressionParse("=*")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = &Expr{ValueI: 10, ValueS: "", Step: 0, Len: 0, Mode: "ValueInt", Symbol: ""}
	result, err = ExpressionParse("=10")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = &Expr{ValueI: 0, ValueS: "", Step: 10, Len: 256, Mode: "RandomStep", Symbol: ""}
	result, err = ExpressionParse("=10l256*")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = &Expr{ValueI: 0, ValueS: "", Step: 0, Len: 512, Mode: "Random", Symbol: ""}
	result, err = ExpressionParse("=l512*")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = &Expr{ValueI: 0, ValueS: "", Step: 10, Len: 0, Mode: "RandomStep", Symbol: "ёж"}
	result, err = ExpressionParse("=10sёж.*")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = &Expr{ValueI: 0, ValueS: "", Step: 0, Len: 0, Mode: "Random", Symbol: "test"}
	result, err = ExpressionParse("=stest.*")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = &Expr{ValueI: 0, ValueS: "", Step: 10, Len: 256, Mode: "RandomStep", Symbol: "label"}
	result, err = ExpressionParse("=10l256slabel.*")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = &Expr{ValueI: 0, ValueS: "", Step: 0, Len: 512, Mode: "Random", Symbol: "ёжик"}
	result, err = ExpressionParse("=l512sёжик.*")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = nil
	result, err = ExpressionParse("=")
	require.Error(t, err)
	require.Equal(t, expression, result)
	expression = nil
	result, err = ExpressionParse("10*")
	require.Error(t, err)
	require.Equal(t, expression, result)
}
