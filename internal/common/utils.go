package common

type Job struct {
	Type     string       // type -> create add find read delete
	DB_path  string       // path to db, by example ./db1, ../databases/test_db, /home/user/db
	CreateDB DBDesc       // struct, describing fields and index database to be created
	AddRec   []FieldValue // struct with data, to be added in database
	Find     []FieldValue // struct wit query for finding in database
	NumRec   int          // number record
	Debug    int          // indication that debugging needed
	DataFile string       // file with data
}

type JobCreate struct {
	DB_path  string // path to db, by example ./db1, ../databases/test_db, /home/user/db
	CreateDB DBDesc // struct, describing fields and index database to be created
}

type JobAdd struct {
	DB_path string // path to db, by example ./db1, ../databases/test_db, /home/user/db

	AddRecs []struct {
		Rec []FieldValue // struct with data, to be added in database
	}
}

type JobFind struct {
	DB_path string       // path to db, by example ./db1, ../databases/test_db, /home/user/db
	FindRec []FieldValue // struct wit query for finding in database
}

type JobRead struct {
	DB_path string // path to db, by example ./db1, ../databases/test_db, /home/user/db
	NumRec  int64  // record id
}

type JobDelete struct {
	DB_path string // path to db, by example ./db1, ../databases/test_db, /home/user/db
	NumRec  int64  // record id
}

type JobResult struct {
	Type    string   // type -> create add find read delete
	Result  string   // next values: OK for successful, Error if error, where text error bn field Error
	Error   string   // error text
	Records []Record // result records
}

type DBDesc struct {
	Indexes []Index
	Fields  []Field
}
