package sc

import (
	"fmt"
	"reflect"
	"github.com/pkg/errors"
)

// Overall db container. There can be multiple of these
type Database struct {
	Name string
	Tables map[string]Table

}

// Defines what a table is. Basically just maps which serve as indexes to underlying data
type Table struct {
	Name string
	Indexes map[string]Index

}

type Index struct {
	Idx map[interface{}]interface{}
}

//TODO do we even want the concept of Db or table
// TODO ensure name is unique
func InitDb(name string) Database {
	db := Database{Name: name, Tables: make(map[string]Table)}
	return db
}

// Add a table to the db if it hasn't already been added
// Set an empty table index map too which will be filled with data
// during the Table.AddData process
func (db Database) AddTable(tableName string, indexes... string) (Table, error) {
	if _, ok := db.Tables[tableName]; ok {
		return db.Tables[tableName], fmt.Errorf("Table %s already exists in db %s", tableName, db.Name)
	}
	// TODO need to set the indexes for this table too
	idxMap := make(map[string]Index)
	for _, idx := range indexes {
		idxMap[idx] = Index{Idx: make(map[interface{}]interface{})}
		//idxMap[idx] = Index{Idx: make(map[interface{}]*interface{})}
	}

	table := Table{Name: tableName, Indexes: idxMap}
	db.Tables[tableName] = table

	return table, nil
}

// For each type of index we've set on this table (during table creation) link to the data
// Can do a bulk insert by passing in the data as a slice of interfaces{}
// TODO can we handle non-unique indexes? or how do we want to do that
//	current implementation will overwrite, but should we at least warn user about that
//    can have future feature where you can turn uniqueness on at table or even table:index level
//func (tbl Table) AddData(data... interface{}) error {
//
//	for _, d := range data {
//		if !HasRequiredIndexes(tbl, d) {
//			return fmt.Errorf("Data obj %s doesn't have all necessary indexes in %s", d, tbl.Name)
//		}
//		//fmt.Println("data", d)
//		for k := range tbl.Indexes {
//			//fmt.Println(k, v)
//			idxMap := tbl.Indexes[k]
//			// find corresponding type/field in data
//			refValOf := reflect.ValueOf(d)
//			val := reflect.Indirect(refValOf)
//			for i := 0; i < val.NumField(); i++ {
//				fieldName := val.Type().Field(i).Name
//				fieldVal := val.Field(i).Interface()
//				//fmt.Println("This is field Name and Val", fieldName, fieldVal)
//				if k == fieldName {
//					idxMap.Idx[fieldVal] = d
//				}
//			}
//
//		}
//		//fmt.Println("NEW DATA", d)
//		//fmt.Println("NEW TABLE", tbl)
//	}
//	return nil
//}

// Does the actual work of adding data objects to a table. Does not check before overwriting existing data since
// that is the job of any methods calling this one.
// This automatically figures out what the indexes are based on the table definition
func (tbl Table) addData(data... interface{}) error {
	// TODO would this be any faster if I made a separate go routine for each data object in the slice??
	// Potentially see https://hackernoon.com/dancing-with-go-s-mutexes-92407ae927bf for tips on syncing
	// or https://blog.golang.org/share-memory-by-communicating for using channels and go routines together
	for _, d := range data {
		if !HasRequiredIndexes(tbl, d) {
			return fmt.Errorf("Data obj %s doesn't have all necessary indexes in %s", d, tbl.Name)
		}
		fmt.Println("data", d, &d, reflect.TypeOf(d))
		for k := range tbl.Indexes {
			//fmt.Println(k, v)
			idxMap := tbl.Indexes[k]
			// find corresponding type/field in data
			refValOf := reflect.ValueOf(d)
			val := reflect.Indirect(refValOf)
			for i := 0; i < val.NumField(); i++ {
				fieldName := val.Type().Field(i).Name
				fieldVal := val.Field(i).Interface()
				//fmt.Println("This is field Name and Val", fieldName, fieldVal)
				if k == fieldName {
					idxMap.Idx[fieldVal] = d
				}
			}
		}
		//fmt.Println("NEW DATA", d)
		fmt.Println("NEW TABLE", tbl)
		tbl.PrettyPrint()
	}
	return nil
}

// Inserts new Data objects if they don't already exist. Will fail if data already exists for a given key.
// If slice contains data with overlapping keys then only one will win out in a non-deterministic fashion
func (tbl Table) InsertData(data... interface{}) error {

	for _, d := range data {
		// find
		fmt.Println("data", d)
		refValOf := reflect.ValueOf(d)
		val := reflect.Indirect(refValOf)
		for idx := range tbl.Indexes {

			for i := 0; i < val.NumField(); i++ {
				//fieldName := val.Type().Field(i).Name
				fieldVal := val.Field(i).Interface()
				fmt.Println("idx, value", idx, reflect.TypeOf(idx), fieldVal)
				lookupVal := tbl.LookupKey(fieldVal, idx)
				fmt.Println("Insert data", len(data), idx, lookupVal)

				if lookupVal != nil {
					fmt.Println("Data already exists")
					tbl.PrettyPrint()
					return errors.New("Data already exists")
				}
			}
		}
		// Only add if keys are unique on ALL indexes in the map
		// Can only do this serially currently otherwise if you pass in the same keys in the same slice
		// there will be trouble. Can maybe synchronize with channels/mutex??
		tbl.addData(d)
	}
	tbl.PrettyPrint()
	return nil
}

// Convenience function which is a thin wrapper around AddData()
// Overwrites existing data if it's there and inserts data if it didn't previously exist
func (tbl Table) SetData(data... interface{}) error {
	return tbl.addData(data...)
}

// For a given struct creates a map of struct field names to values
// TODO hook this up to other loops in the code
func getStructFieldAndVal(data interface{}) map[string]interface{} {
	refVal := reflect.ValueOf(data)
	val := reflect.Indirect(refVal)
	resultMap := make(map[string]interface{})
	for i := 0; i < val.NumField(); i++ {
		fieldName := val.Type().Field(i).Name
		fieldVal := val.Field(i).Interface()
		resultMap[fieldName] = fieldVal
	}
	return resultMap
}

// For a given data object see if it already exists in the table by checking all the table indexes
func (tbl Table) doAllKeysExist(data interface{}) bool {
	structMap := getStructFieldAndVal(data)
	for idx := range tbl.Indexes {
		if tbl.LookupKey(structMap[idx], idx) == nil {
			return false
		}
	}
	return true

}

// Only update data if it already exists as a key
// If the key doesn't exist it will fail to add that piece of data
func (tbl Table) UpdateData(data... interface{}) error {
	for _, d := range data {
		keysExist := tbl.doAllKeysExist(d) // will prob need to make a function for reflecting field/value
		if keysExist {
			tbl.addData(d)
		} else {
			return errors.Errorf("UpdateData DNE: %s", d)
		}
	}
	return nil
}

func (tbl Table) LookupKey(key interface{}, idx string) interface{} {
	return tbl.Indexes[idx].Idx[key]
}

// Totally remove the table from the db ie. remove table key from db map
// Does not remove the underlying data object since they are just stored pointers
// TODO figure out if use case would be to delete underlying data too
// TODO figure out if doing this will lead to memory leaks
func (db Database) DropTable(tableName string) {
	delete(db.Tables, tableName)
}

// Keeps the table as a key in the map, but removes all values associated with it
// Doesn't actually delete the underlying data objects or indexes.
func (tbl Table) CleanTableData() {
	for idx := range tbl.Indexes {
		tbl.Indexes[idx] = Index{Idx: make(map[interface{}]interface{})}
	}
}


// TODO do we need this function?
func (idx Index) findByKey(key interface{}) interface{} {
	return idx.Idx[key]
}

func (tbl Table) PrettyPrint() {
	fmt.Println("TABLE")
	for k, v := range tbl.Indexes {
		fmt.Println("Index:", k)
		for k1, v1 := range v.Idx {
			fmt.Printf("\t%s :: %s\n", k1, v1)
		}
	}
}

// Given a table return how many data objects are stored.
// This gets the count by checking the length of one of the indexes. Uses the assumption that  each index has the
// same number of data objects per table since that is how the model works.
func GetTableSize(table Table) int {
	for k := range table.Indexes {
		return len(table.Indexes[k].Idx)
	}
	return 0
}

// Given a table and a data object, determine if the data object has at a minimum all the indexes for the table
func HasRequiredIndexes(table Table, data interface{}) bool {
	for k := range table.Indexes {
		found := false
		refValOf := reflect.ValueOf(data)
		val := reflect.Indirect(refValOf)
		for i := 0; i < val.NumField(); i++ {
			fieldName := val.Type().Field(i).Name
			if k == fieldName {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Return all the table names in a given database as a slice
// TODO benchmark and see how compares to doing this with i := 0 counter rather than range
// eg http://stackoverflow.com/questions/21362950/golang-getting-a-slice-of-keys-from-a-map claims that would be faster than a range with append
func (db Database) ListTableNames() []string {
	tableList := make([]string, 0, len(db.Tables))
	for k := range db.Tables {
		tableList = append(tableList, k)
	}
	return tableList
}

// Return all the index names in a given table as a slice
// TODO benchmark and see how compares to doing this with i := 0 counter rather than range
// eg http://stackoverflow.com/questions/21362950/golang-getting-a-slice-of-keys-from-a-map claims that would be faster than a range with append
func (tbl Table) ListIndexNames() []string {
	indexList := make([]string, 0, len(tbl.Indexes))
	for k := range tbl.Indexes {
		indexList = append(indexList, k)
	}
	return indexList
}

