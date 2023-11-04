package index

type IndexConfig struct {
	FieldsName []string `json:"fields_name"`
	Free       bool
	Mask       int64
}

type SmallDBConfig struct {
	DataFileName             string                 `json:"data_file_name"`
	IndexFilesName           []string               `json:"index_files_name"`
	FreeIndexFilesName       []string               `json:"free_index_files_name"`
	BlocksFileName           string                 `json:"blocks_file_name"`
	DeletedDataFileName      string                 `json:"deleted_data_file_name"`
	DeletedDataIndexFileName string                 `json:"deleted_data_index_file_name"`
	JournalFileName          string                 `json:"journal_file_name"`
	RowIndexFileName         string                 `json:"row_index_file_name"`
	BlockSize                int32                  `json:"block_size"`
	HashTableSize            uint32                 `json:"hash_table_size"`
	UseSync                  int8                   `json:"use_sync"`
	UseJournal               int8                   `json:"use_journal"`
	UseDeletedData           int8                   `json:"use_deleted_data"`
	DatabaseName             string                 `json:"database_name"`
	FieldsName               []string               `json:"fields_name"`
	Indexes                  []IndexConfig          `json:"indexes"`
	IndexesMap               map[string]IndexConfig `json:"indexes_map"`
}
