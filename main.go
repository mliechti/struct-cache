package main

import (
	//"fmt"
	"godb/sc"
	//"reflect"
)


type testObj struct {
	Id string
	Username string
}
// An ill-conceived attempt to write a key-value, in-memory data store which sacrifices memory for speed
// Data is stored in data objects which are the source of truth for a given piece of data.
// These DataObjects can be looked up in a set of hash maps which allow for quick access of the data
// An arbitrary number of these hash maps can be created
// Does not handle compound indexes - this is key value only - can potentially implement this using a struct as a key though
// You can mix and match structs in a given table so long as each struct has all the minimum index fields
// All struct fields must be exported, ie. uppercased otherwise they can't be used => panic in the reflection code

// FAQ
// Doesn't library XYZ already do this?
// Probably
// Isn't it also a lot better?
// Yes
// Why should I use this one?
// You shouldn't really. Maybe if you have a brain thing you might want to, though even then you still shouldn't.
// I mean this is actually pretty bad and I think might be making me sick.
// That is likely. Do not use this code under any circumstances or look directly at this code. If for some reason you
// must try it, please use only in a well ventilated area and under strict supervision of a professional.  Long exposure
// can lead to bloating, swelling, blindness, impotence, and in some cases death.
// *These statements have not been evaluated by the FDA

func main() {
	t := testObj{"23432asdflkj", "Mark1"}
	db := sc.InitDb("Master DB")
	table, _ := db.AddTable("users", "Id", "Username")
	table.SetData(t)

}
