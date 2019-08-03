package sqldb

import (
	"fmt"
	"os"
	"testing"

	"github.com/semog/gocommon"
)

// In testspace, no one can hear you fail.
const testFolderName = ".testspace"
const testDbName = "TestDb"

func setupTests(t *testing.T) {
	removeTempFiles(t)
}

func cleanupTests(t *testing.T) {
	removeTempFiles(t)
}

func removeTempFiles(t *testing.T) {
	if gocommon.FileExists(testDbName) {
		os.Remove(testDbName)
	}
}

func openTestDb(t *testing.T) *SQLDb {
	sdb, err := OpenDb(testDbName)
	if err != nil {
		t.Errorf("OpenDb error: %v", err)
	}
	// Should still have a valid object
	if sdb == nil {
		t.Error("OpenDb returned nil SQLDb")
	}

	return sdb
}

func openPatchedTestDb(t *testing.T) *SQLDb {
	sdb, err := OpenAndPatchDb(testDbName, nil)
	if err != nil {
		t.Errorf("OpenDb error: %v", err)
	}
	// Should still have a valid object
	if sdb == nil {
		t.Error("OpenDb returned nil SQLDb")
	}

	return sdb
}

func closeDb(t *testing.T, sdb **SQLDb) {
	if *sdb != nil {
		err := (*sdb).Close()
		if err != nil {
			t.Errorf("Close database error: %v", err)
		}
	}
	sdb = nil
}

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	saveFolder, _ := os.Getwd()
	os.Mkdir(testFolderName, os.ModeDir|os.ModePerm)
	os.Chdir(testFolderName)
	ret := m.Run()
	os.Chdir(saveFolder)
	os.RemoveAll(testFolderName)
	os.Exit(ret)
}

func TestOpenAndPatchDb_NoPatchFunctions(t *testing.T) {
	setupTests(t)
	defer cleanupTests(t)

	sdb, err := OpenAndPatchDb(testDbName, nil)
	if err != nil {
		t.Errorf("OpenAndPatchDb with nil patch functions: %v", err)
	}
	if sdb == nil {
		t.Error("OpenAndPatchDb returned nil SQLDb")
	}
}

func TestOpenAndPatchDb_WithPatchFunctions(t *testing.T) {
	setupTests(t)
	defer cleanupTests(t)

	patch1Called := false
	patch2Called := false
	// The array of patch functions that will automatically upgrade the database.
	dbPatchFuncs := []PatchFuncType{
		{PatchID: 1, PatchFunc: func(sdb *SQLDb) error {
			patch1Called = true
			return nil
		}},
		{PatchID: 2, PatchFunc: func(sdb *SQLDb) error {
			patch2Called = true
			return nil
		}},
	}
	sdb, err := OpenAndPatchDb(testDbName, dbPatchFuncs)
	if err != nil {
		t.Errorf("OpenAndPatchDb with patch functions: %v", err)
	}
	if sdb == nil {
		t.Error("OpenAndPatchDb returned nil SQLDb")
	}
	if !patch1Called {
		t.Error("Did not call patch 1")
	}
	if !patch2Called {
		t.Error("Did not call patch 2")
	}

	// Close the database, and re-open it with new set of patch functions.
	closeDb(t, &sdb)
	if err != nil {
		// Don't continue with this test. It won't get any better.
		return
	}

	// Only the third patch function should be called.
	patch1Called = false
	patch2Called = false
	patch3Called := false
	dbPatchFuncs = []PatchFuncType{
		{PatchID: 1, PatchFunc: func(sdb *SQLDb) error {
			patch1Called = true
			return nil
		}},
		{PatchID: 2, PatchFunc: func(sdb *SQLDb) error {
			patch2Called = true
			return nil
		}},
		{PatchID: 3, PatchFunc: func(sdb *SQLDb) error {
			patch3Called = true
			return nil
		}},
	}
	sdb, err = OpenAndPatchDb(testDbName, dbPatchFuncs)
	defer closeDb(t, &sdb)
	if err != nil {
		t.Errorf("OpenAndPatchDb with patch functions: %v", err)
	}
	if sdb == nil {
		t.Error("OpenAndPatchDb returned nil SQLDb")
	}
	if patch1Called {
		t.Error("Re-called patch 1")
	}
	if patch2Called {
		t.Error("Re-called patch 2")
	}
	if !patch3Called {
		t.Error("Did not call patch 3")
	}
}

func TestOpenAndPatchDb_WithPatchFunctionError(t *testing.T) {
	setupTests(t)
	defer cleanupTests(t)

	// The array of patch functions that will automatically upgrade the database.
	dbPatchFuncs := []PatchFuncType{
		{PatchID: 1, PatchFunc: func(sdb *SQLDb) error {
			return fmt.Errorf("Error patching")
		}},
	}
	sdb, err := OpenAndPatchDb(testDbName, dbPatchFuncs)
	defer closeDb(t, &sdb)
	if err == nil {
		t.Error("OpenAndPatchDb did not return patch function error")
	}
	// Even with patch error, should still have a valid object
	if sdb == nil {
		t.Error("OpenAndPatchDb returned nil SQLDb")
	}
}

func TestCreateDropTable(t *testing.T) {
	setupTests(t)
	defer cleanupTests(t)

	sdb := openTestDb(t)
	defer closeDb(t, &sdb)
	err := sdb.CreateTable("testtable (id INTEGER, field1 TEXT, field2 TEXT)")
	if err != nil {
		t.Errorf("CreateTable error: %v", err)
	}

	err = sdb.DropTable("testtable")
	if err != nil {
		t.Errorf("DropTable error: %v", err)
	}
}

func TestDropNonExistingTable(t *testing.T) {
	setupTests(t)
	defer cleanupTests(t)

	sdb := openTestDb(t)
	defer closeDb(t, &sdb)
	err := sdb.DropTable("notatable")
	if err != nil {
		t.Errorf("DropTable error: %v", err)
	}
}

func TestCreateIndex(t *testing.T) {
	setupTests(t)
	defer cleanupTests(t)

	sdb := openTestDb(t)
	defer closeDb(t, &sdb)
	err := sdb.CreateTable("testtable (id INTEGER, field1 TEXT, field2 TEXT)")
	if err != nil {
		t.Errorf("CreateTable error: %v", err)
	}

	err = sdb.CreateIndex("test_idx ON testtable (id)")
	if err != nil {
		t.Errorf("CreateIndex error: %v", err)
	}
}

func TestCreateIndexNonExistingTable(t *testing.T) {
	setupTests(t)
	defer cleanupTests(t)

	sdb := openTestDb(t)
	defer closeDb(t, &sdb)

	err := sdb.CreateIndex("test_idx ON notatable (id)")
	if err == nil {
		t.Error("CreateIndex did not return an error")
	}
}
func testGkey(t *testing.T, err error, expected, actual int) {
	if err != nil {
		t.Errorf("GetKey error: %v", err)
	}

	if actual != expected {
		t.Errorf("Expected gkey to be %v, but was %v", expected, actual)
	}
}

func TestGetGkey(t *testing.T) {

	setupTests(t)
	defer cleanupTests(t)

	sdb := openPatchedTestDb(t)
	defer closeDb(t, &sdb)

	gkey, err := sdb.GetGkey()
	// A new database should have gkey start at 1.
	testGkey(t, err, 1, gkey)

	gkey, err = sdb.GetGkey()
	testGkey(t, err, 2, gkey)

	// Close and re-open the database
	closeDb(t, &sdb)
	sdb = openTestDb(t)

	gkey, err = sdb.GetGkey()
	testGkey(t, err, 3, gkey)
}
