package sc_test

import (
	"godb/sc"
	"testing"
	"os"
	"reflect"
	"fmt"
	"time"
	"sort"
)

// To execute tests run: go test ./... -v
// To benchmark with memory: go test ./sc/... -v -bench=. -benchmem
func TestMain(m *testing.M) {


	// Run the other tests
	os.Exit(m.Run())
}


func TestInitDb(t *testing.T) {
	dbName := "test1"
	emptyTable := make(map[string]sc.Table)
	db1 := sc.InitDb(dbName)
	if db1.Name != dbName || len(db1.Tables) != 0 || reflect.TypeOf(db1.Tables) != reflect.TypeOf(emptyTable) {
		t.Fail()
	}
}

func TestAddTable(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, err := db.AddTable(tableName, "Username", "Id")
	emptyIndex1 := make(map[string]sc.Index)
	emptyIndex1["Username"] = sc.Index{}
	emptyIndex1["Id"] = sc.Index{}
	// check can add new table
	if err != nil || table.Name != tableName || len(table.Indexes) != len(emptyIndex1) || reflect.TypeOf(table.Indexes) != reflect.TypeOf(emptyIndex1) {
		fmt.Println("Unable to add 1st table", table.Name, tableName, len(table.Indexes))
		t.Fail()
	}

	// check can't add same table twice
	table, err = db.AddTable(tableName)
	if err == nil {
		fmt.Println("Error adding same table name twice")
		t.Fail()
	}

	// but should be able to add another table name
	tableName2 := "test table 2"
	table, err = db.AddTable(tableName2, "Id", "Created_At", "Count")
	emptyIndex2 := make(map[string]sc.Index)
	emptyIndex2["Created_At"] = sc.Index{}
	emptyIndex2["Count"] = sc.Index{}
	emptyIndex2["Id"] = sc.Index{}
	// check can add new table
	if err != nil || table.Name != tableName2 || len(table.Indexes) != len(emptyIndex2) || reflect.TypeOf(table.Indexes) != reflect.TypeOf(emptyIndex2) {
		fmt.Println("Unable to add 2nd table", tableName, tableName2)
		t.Fail()
	}
}



// Given a struct and an already created Table this will call the SetData function and validate the results
// sc.Table will have the indexes you need to test
func testSetDataHelper(table sc.Table, testObj... interface{}) bool {

	tableLen := sc.GetTableSize(table) // keep count for later
	err := table.SetData(testObj...)
	if err != nil {
		fmt.Println(err)
		return false
	}
	for k := range table.Indexes {
		for _, tObj := range testObj {
			refValOf := reflect.ValueOf(tObj)
			val := reflect.Indirect(refValOf)

			for i := 0; i < val.NumField(); i++ {
				fieldName := val.Type().Field(i).Name
				fieldVal := val.Field(i).Interface()
				if k == fieldName {
					if !reflect.DeepEqual(table.Indexes[k].Idx[fieldVal], tObj) {
						fmt.Printf("FAIL: TestSetData retrieved by %s failed\n ", k)
						return false
					}
					if len(table.Indexes[k].Idx) != tableLen + len(testObj) {
						fmt.Printf("FAIL: TestSetData Idx count wrong %d vs %d\n", len(table.Indexes[k].Idx), tableLen + len(testObj))
						return false
					}
				}
			}
		}
	}
	return true

}

// TODO refactor this to validate/reflect in a loop - maybe break some parts into subfunctions too
func TestSetData(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, err := db.AddTable(tableName, "Id", "Username")
	if err != nil {
		t.Fail()
	}

	// add vanilla data object
	type testObj struct {
		Id string
		Username string
	}

	objId := "tobj1"
	objUsername := "test_user1"
	tObj := testObj{Id: objId, Username: objUsername}

	result := testSetDataHelper(table, tObj)
	if !result {
		t.Fail()
	}

	// add another of same type
	objId2 := "tobj2"
	objUsername2 := "test_user2"
	tObj2 := testObj{Id: objId2, Username: objUsername2}

	result = testSetDataHelper(table, tObj2)
	if !result {
		t.Fail()
	}

	// add another object of a different type but still has index fields
	type testObj3 struct {
		Id string
		Username string
		Created_At time.Time
		Count int
	}

	objId3 := "tobj3"
	objUsername3 := "test_user3"
	tObj3 := testObj3{Id: objId3, Username: objUsername3, Created_At: time.Now().UTC(), Count: 1}
	result = testSetDataHelper(table, tObj3)
	if !result {
		t.Fail()
	}


	// add multiple data objects at the same time as a 2nd table in the same db as before
	tableName2 := "testTable2"
	table2, err := db.AddTable(tableName2, "Id", "Username")
	if err != nil {
		t.Fail()
	}
	result = testSetDataHelper(table2, tObj, tObj2, tObj3)
	if !result {
		t.Fail()
	}
	if !reflect.DeepEqual(table.Indexes, table2.Indexes) {
		fmt.Println("FAIL: TestSetData4 tables aren't equivalent!")
		t.Fail()
	}

	// add an object that is missing one of the necessary indexes
	type testObjMissingIndex struct {
		Field1 string
		Field2 int
	}
	tObjMissingIndex := testObjMissingIndex{"field1", 10}
	result = testSetDataHelper(table2, tObjMissingIndex)
	if result {
		fmt.Println("FAIL: TestSetData data obj with missing index fields was not caught")
		t.Fail()
	}
}

// TODO would also need to test that adding tables are synced to avoid race conditions
func testAddDataSynchronization(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, err := db.AddTable(tableName, "Id", "Username")
	if err != nil {
		t.Fail()
	}

	// add vanilla data object
	type testObj struct {
		Id string
		Username string
	}

	objId := "tobj1"
	objUsername := "test_user1"
	tObj := testObj{Id: objId, Username: objUsername}
	fmt.Println("SYNC TEST!!!!!!!!!!!!!!")
	result := testSetDataHelper(table, tObj)
	if !result {
		t.Fail()
	}
}

func TestInsertData(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, err := db.AddTable(tableName, "Id", "Username")
	if err != nil {
		t.Fail()
	}

	// add vanilla data object
	type testObj struct {
		Id string
		Username string
	}

	objId := "objID1"
	objUsername := "test_user1"
	tObj := testObj{Id: objId, Username: objUsername}
	result := table.InsertData(tObj)
	if result != nil {
		fmt.Println("FAIL: Insert initial data")
		t.Fail()
	}

	// add same struct on same key1 - should fail
	objId = "objID1"
	objUsername = "test_user2"
	tObj = testObj{Id: objId, Username: objUsername}
	result = table.InsertData(tObj)
	if result == nil {
		fmt.Println("FAIL: Insert same struct on same key1")
		t.Fail()
	}

	// add same struct on same key2 - should fail
	objId = "objID2"
	objUsername = "test_user1"
	tObj = testObj{Id: objId, Username: objUsername}
	result = table.InsertData(tObj)
	if result == nil {
		fmt.Println("FAIL: Insert same struct on same key2")
		t.Fail()
	}
	// add same struct on new key - should work
	objId = "objID2"
	objUsername = "test_user2"
	tObj = testObj{Id: objId, Username: objUsername}
	result = table.InsertData(tObj)
	if result != nil {
		fmt.Println("FAIL: Insert same struct on new key")
		t.Fail()
	}

	// add different struct on same key1 - should fail
	type testObj2 struct {
		Id string
		Username string
		Misc int
	}
	objId = "objID1"
	objUsername = "test_user3"
	tObj2 := testObj2{Id: objId, Username: objUsername, Misc: 7}
	result = table.InsertData(tObj2)
	if result == nil {
		fmt.Println("FAIL: Insert new struct on same key1")
		t.Fail()
	}

	// add different struct on same key2 - should fail
	objId = "objID3"
	objUsername = "test_user1"
	tObj2 = testObj2{Id: objId, Username: objUsername, Misc: 7}
	result = table.InsertData(tObj2)
	if result == nil {
		fmt.Println("FAIL: Insert new struct on same key2")
		t.Fail()
	}
	// add different struct on new key - should work
	objId = "objID3"
	objUsername = "test_user3"
	tObj2 = testObj2{Id: objId, Username: objUsername, Misc: 7}
	result = table.InsertData(tObj2)
	if result != nil {
		fmt.Println("FAIL: Insert new struct on new key")
		t.Fail()
	}
}

// Test that data insertion is atomic so we don't end up with inconsistent data
func TestInsertDataAtomically(t *testing.T) {
	// overlap insertion of following structs
	// struct1: {Id: "Id1", Username: "User1", Misc: 1} and struct2: {Id: "Id1", Username: "User1", Misc: 2}
	// We will start insertion of struct1 first so we should expect to end up with only struct1 data since
	// struct 2 should be blocked and then fail to insert since no overwriting keys on insertion

	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, err := db.AddTable(tableName, "Id", "Username")
	if err != nil {
		t.Fail()
	}
	type testObj struct {
		Id string
		Username string
		Misc int
	}

	objId1 := "Id1"
	objUser1 := "User1"


	obj1 := testObj{Id: objId1, Username: objUser1, Misc: 1}
	obj2 := testObj{Id: objId1, Username: objUser1, Misc: 2}

	table.InsertData(obj1)
	table.InsertData(obj2)

	if table.LookupKey(objId1, "Id").(testObj).Misc != 1 {
		fmt.Println("FAIL: Insert atomic data Id")
		t.Fail()
	}
	if table.LookupKey(objUser1, "Username").(testObj).Misc != 1 {
		fmt.Println("FAIL: Insert atomic data Username")
		t.Fail()
	}
	fmt.Println("ATOMIC TEST")
	table.PrettyPrint()


	// Test where obj1 and obj2 are inserting as the same slice eg InsertData(obj1, obj2)
	// TODO can we make this logic simpler?
	table.CleanTableData()
	table.InsertData(obj1, obj2)
	if !(table.LookupKey(objId1, "Id").(testObj).Misc == 1 && table.LookupKey(objUser1, "Username").(testObj).Misc == 1 ||
		table.LookupKey(objId1, "Id").(testObj).Misc == 2 && table.LookupKey(objUser1, "Username").(testObj).Misc == 2) {

		fmt.Println("FAIL: Batch Insert atomic data Id")
		t.Fail()
	}


}

func TestCleanTableData(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, err := db.AddTable(tableName, "Id", "Username")
	if err != nil {
		t.Fail()
	}

	// Clean table with no data - shouldn't be any issues
	table.CleanTableData()
	for idx := range table.Indexes {
		if len(table.Indexes[idx].Idx) > 0 {
			fmt.Println("FAIL: Clean table with no data", idx)
			t.Fail()
		}
	}


	// Create some data for later tests
	type testObj struct {
		Id string
		Username string
		Misc int
	}

	objId1 := "Id1"
	objUser1 := "User1"
	obj1 := testObj{Id: objId1, Username: objUser1, Misc: 1}
	before := table.LookupKey(objId1, "Id")
	table.InsertData(obj1)

	// Clean table with data
	table.CleanTableData()
	after := table.LookupKey(objId1, "Id")
	for idx := range table.Indexes {
		if len(table.Indexes[idx].Idx) > 0 {
			fmt.Println("FAIL: Clean table with data len > 0", idx)
			t.Fail()
		}
		if !reflect.DeepEqual(before, after) {
			fmt.Println("FAIL: Clean table with data non empty map", idx)
			t.Fail()
		}
	}
}

func TestDropTable(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, err := db.AddTable(tableName, "Id", "Username")
	if err != nil {
		t.Fail()
	}

	// Drop table with no data
	db.DropTable(tableName)
	if len(db.Tables) > 0 {
		fmt.Println("FAIL: Drop table with no data")
		t.Fail()
	}

	// Create some data for later tests
	table, err = db.AddTable(tableName, "Id", "Username")
	if err != nil {
		t.Fail()
	}
	type testObj struct {
		Id string
		Username string
		Misc int
	}

	objId1 := "Id1"
	objUser1 := "User1"
	obj1 := testObj{Id: objId1, Username: objUser1, Misc: 1}
	table.InsertData(obj1)

	// Drop table that doesn't exist - should not panic
	db.DropTable("nonexistingtablename")
	if len(db.Tables) != 1 {
		fmt.Println("FAIL: Drop table that DNE")
		t.Fail()
	}

	// Drop table with data
	db.DropTable(tableName)
	if len(db.Tables) > 0 {
		fmt.Println("FAIL: Drop table with data")
		t.Fail()
	}
}

func TestHasRequiredIndexes(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, err := db.AddTable(tableName, "Id", "Username")
	if err != nil {
		t.Fail()
	}

	type testObjBareMinimum struct {
		Id string
		Username string
	}
	objBareMinimum := testObjBareMinimum{Id: "t1", Username: "xxx"}

	type testObjExceedsMinimum struct {
		Id string
		Username string
		Created_At time.Time
		Count int
	}
	objExceedsMinimum := testObjExceedsMinimum{Id: "t2", Username: "xxx", Created_At: time.Now().UTC(), Count: 5}

	type testObjOnlyOneOverlap struct {
		Id string
		Created_At time.Time
		Count int
	}
	objOnlyOneOverlap := testObjOnlyOneOverlap{Id: "t3", Created_At: time.Now().UTC(), Count: 100}

	type testObjNoOverlap struct {
		Created_At time.Time
		Count int
	}
	objNoOverlap := testObjNoOverlap{Created_At: time.Now().UTC(), Count: 77}

	result := sc.HasRequiredIndexes(table, objBareMinimum)
	if !result {
		fmt.Println("objBareMinimum failed check")
		t.Fail()
	}
	result = sc.HasRequiredIndexes(table, objExceedsMinimum)
	if !result {
		fmt.Println("objExceedsMinimum failed check")
		t.Fail()
	}
	result = sc.HasRequiredIndexes(table, objOnlyOneOverlap)
	if result {
		fmt.Println("objOnlyOneOverlap failed check")
		t.Fail()
	}
	result = sc.HasRequiredIndexes(table, objNoOverlap)
	if result {
		fmt.Println("objNoOverlap failed check")
		t.Fail()
	}

}

func TestListIndexNames(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	modelIndexList := []string{"Id", "Username"}
	table, err := db.AddTable(tableName, modelIndexList...)
	if err != nil {
		t.Fail()
	}

	indexList := table.ListIndexNames()
	// sort for easy comparison
	sort.Strings(indexList)
	sort.Strings(modelIndexList)
	if !reflect.DeepEqual(indexList, modelIndexList) {
		fmt.Printf("FAIL: TestListIndexNames %s doesn't match %s\n", indexList, modelIndexList)
		t.Fail()
	}
}

func TestListTableNames(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	modelTableNameList := []string{"Table1", "Table2", "Table3"}

	modelIndexList := []string{"Id", "Username"}
	for _, tbl := range modelTableNameList {
		_, err := db.AddTable(tbl, modelIndexList...)
		if err != nil {
			t.Fail()
		}
	}

	tableNameList := db.ListTableNames()
	// sort for easy comparison
	sort.Strings(tableNameList)
	sort.Strings(modelTableNameList)
	if !reflect.DeepEqual(tableNameList, modelTableNameList) {
		fmt.Printf("FAIL: TestListTableNames %s doesn't match %s\n", tableNameList, modelTableNameList)
		t.Fail()
	}
}

// Make sure the dicts are storing pointers to the data objects and not duplicating the data objects every time which
// would be too memory intensive
//func BenchmarkUsingPointers(b *testing.B) {
func TestUsingPointers(t *testing.T) {
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, _ := db.AddTable(tableName, "Id", "Username")

	// add vanilla data object
	type testObj struct {
		Id string
		Username string
	}

	objId := "tobj1"
	objId2 := "22tobj22"
	objUsername := "test_user1"
	objUsername2 := "22test_user22"
	tObj := testObj{Id: objId, Username: objUsername}
	tObj2 := testObj{Id: objId2, Username: objUsername2}

	// TODO this is best as a pointer to save memory??  if so how do we enforce this?
	table.SetData(&tObj, &tObj2)

	tObj.Username = "yyy"
	fmt.Println("tObj", tObj)
	//table.Indexes["Id"].Idx[objId].Username = "zzz"
	val := table.Indexes["Id"].Idx[objId]
	// TODO easily set and retrieve data without crazy . syntax
	//val.(testObj).Username = "yesss"

	v := table.LookupKey(objId, "Id")

	//val := table.Indexes["Id"]
	val2 := table.Indexes["Id"].Idx[objId2]
	//val2 := table.Indexes["Username"]
	fmt.Println("v", v, reflect.TypeOf(v))
	fmt.Println("table.Indexes['Id'].Idx[objId]", reflect.TypeOf(table.Indexes["Id"].Idx[objId]))
	fmt.Println("datastruc1", val, "typeof1", reflect.TypeOf(val))
	fmt.Println("datastruc2", val2, "typeof2", reflect.TypeOf(val2))
	table.PrettyPrint()

	//fmt.Println("size in bytes:", unsafe.Sizeof(table))


}

func TestUpdateData(t *testing.T) {

	// Try to update where data doesn't already exist - should fail
	dbName := "testdb"
	db := sc.InitDb(dbName)

	tableName := "testTable"
	table, _ := db.AddTable(tableName, "Id", "Username")

	type testObj struct {
		Id string
		Username string
		Count int
	}

	objId := "tobj1"
	objUsername := "test_user1"
	objCount := 1
	tObj := testObj{Id: objId, Username: objUsername, Count: objCount}

	err := table.UpdateData(tObj)
	if err == nil {
		fmt.Println("FAIL: TestUpdateData updated when data didn't previously exist.")
		t.Fail()
	}
	table.PrettyPrint()

	// Try to update where data already exists - should succeed
	table.InsertData(tObj)
	objCount2 := 2
	tObj2 := testObj{Id: objId, Username: objUsername, Count: objCount2}
	err = table.UpdateData(tObj2)
	if err != nil || table.LookupKey(objId, "Id").(testObj).Count != objCount2 ||
		table.LookupKey(objUsername, "Username").(testObj).Count != objCount2 {
		fmt.Println("FAIL: TestUpdateData wasn't able to update existing data.")
		t.Fail()
	}
	table.PrettyPrint()

}

// TODO
// Add synchronization to avoid inconsistent data
// Ensure we are using pointers rather than copies. Write some tests for this
// Sort data / leaderboard
//	Need: A. slice/array so can access leaderboard in order a[0], a[1], a[2], etc
		// this will point at a data struct
		// type struct Member {
			// Name string
			// Score float64
			// Rank int64 ???  - if i add this then i can have one map of names to scores/ranks to implement zscore and zrank

		// }
		// we can easily look up the name and score from that
		//B. way to find rank based on Name or score
		//C. way to find Members for a given score range (log(n) + m)
		// OK - zscore - return score of member at a given member name (O(1)) - i would do this using a map of names to scores and is O(1) but requires another data structure
		// OK - zrange - return elements in given range of ranks (log(n) + m) - i do this using my above array/slice and is O(1)
		//zrangebyscore - return elements in given range of scores (log(n) + m)
		// OK - zrank - return rank of member (log(n)) - i would do this using a map of names to ranks which is O(1) but requires another data structure

// Exists command
// TTL??
// Compound indexes?? based off a struct eg as done in "Key Types" section of https://blog.golang.org/go-maps-in-action (web counter by country)
// What about adding indexes after the data has already been added
//    -could be expensive operation - need to check all data has that index and then add the data there - seems like O(n)