package common

type Field struct {
	Name string
}

type FieldValue struct {
	Name  string
	Value string
}

type DataValue struct {
	Value []FieldValue
}

type Index struct {
	Fields []string
}

type Record struct {
	Num         int64    // record id
	FieldsValue []string // fields values array
}
