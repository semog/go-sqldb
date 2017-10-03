package sqldb

import (
	"database/sql"
	"fmt"
	"log"
	// Extend the sql.DB structure to the SQLDb structure.
	_ "github.com/mattn/go-sqlite3"
)

const patchSavePointName = "patchupdate"

// SQLDb - SQL Database wrapper with extended patching functions.
type SQLDb struct {
	*sql.DB
}

// PatchFuncType contains unique patch ID and a patch function to run.
type PatchFuncType struct {
	// PatchID is not necessarily sequential. It just needs to be unique, but convention is sequential.
	PatchID int
	// PatchFunc will perform patch operations on the database.
	PatchFunc func(sdb *SQLDb) bool
}

// The array of patch functions that will automatically upgrade the database.
var internalPatchDbFuncs = []PatchFuncType{
	{0, func(sdb *SQLDb) bool {
		return sdb.CreateTable("IF NOT EXISTS version (patchid INTEGER PRIMARY KEY)")
	}},
}

// OpenAndPatchDb - Open and Patch a database if necessary.
func OpenAndPatchDb(dbFilename string, patchFuncs []PatchFuncType) *SQLDb {
	sdb, err := OpenDb(dbFilename)
	if err != nil {
		return nil
	}
	if !sdb.PatchDb(patchFuncs) {
		return nil
	}
	return sdb
}

// OpenDb - Open a database.
func OpenDb(dbFilename string) (*SQLDb, error) {
	var err error
	sdb := &SQLDb{}
	sdb.DB, err = sql.Open("sqlite3", dbFilename)
	if err != nil {
		return nil, err
	}
	return sdb, nil
}

// PatchDb - Patch a database if necessary.
func (sdb *SQLDb) PatchDb(patchFuncs []PatchFuncType) bool {
	// Create the patch tables
	if !sdb.patch(internalPatchDbFuncs) {
		return false
	}
	// Run the user patches
	if !sdb.patch(patchFuncs) {
		return false
	}
	return true
}

func (sdb *SQLDb) patch(patchFuncs []PatchFuncType) bool {
	// Currently this patching function does not check to see when it is
	// finished whether it is running against a _newer_ database. An additional
	// check would need to be done to see if the final committed patchid matches the
	// expected patchid.
	for _, patch := range patchFuncs {
		if !sdb.patched(patch.PatchID) {
			if !sdb.beginPatch() {
				log.Printf("ERROR: Could not begin patch database for version %d.\n", patch.PatchID)
				return false
			}
			if !(patch.PatchFunc(sdb) && sdb.commitPatch(patch.PatchID)) {
				log.Printf("ERROR: Could not patch database for version %d.\n", patch.PatchID)
				sdb.rollbackPatch()
				return false
			}
			log.Printf("INFO: Patched database version %d.\n", patch.PatchID)
		}
	}
	return true
}

// BeginTrans - Begin transaction
func (sdb *SQLDb) BeginTrans() bool {
	return sdb.Exec("BEGIN")
}

// CommitTrans - Commit transaction
func (sdb *SQLDb) CommitTrans() bool {
	return sdb.Exec("COMMIT")
}

// RollbackTrans - Rollback transaction
func (sdb *SQLDb) RollbackTrans() bool {
	return sdb.Exec("ROLLBACK")
}

// CommitOnSuccess - Commit the transaction if the expression evaluates to true.
func (sdb *SQLDb) CommitOnSuccess(success bool) bool {
	if success {
		return sdb.CommitTrans()
	}
	return sdb.RollbackTrans()
}

// CommitSavePointOnSuccess - Commit up to the save point (or merge with parent transaction) if the expression evaluates to true.
func (sdb *SQLDb) CommitSavePointOnSuccess(name string, success bool) bool {
	if success {
		return sdb.CommitSavePoint(name)
	}
	return sdb.RollbackSavePoint(name)
}

func (sdb *SQLDb) patched(patchid int) bool {
	// Check for the patchid in the version table
	return sdb.SingleQuery(fmt.Sprintf("SELECT patchid FROM version WHERE patchid = %d", patchid))
}

func (sdb *SQLDb) beginPatch() bool {
	return sdb.CreateSavePoint(patchSavePointName)
}

func (sdb *SQLDb) commitPatch(patchid int) bool {
	// Add the patchid to the versions table. If it fails, return false.
	return sdb.Exec(fmt.Sprintf("INSERT OR FAIL INTO version (patchid) VALUES (%d)", patchid)) &&
		sdb.CommitSavePoint(patchSavePointName)
}

func (sdb *SQLDb) rollbackPatch() {
	sdb.RollbackTrans()
}

// CreateSavePoint - Create a save point for rollback or commit.
func (sdb *SQLDb) CreateSavePoint(name string) bool {
	return sdb.Exec(fmt.Sprintf("SAVEPOINT %s", name))
}

// CommitSavePoint - Commit up to the named save point, which rolls it up into parent transaction.
func (sdb *SQLDb) CommitSavePoint(name string) bool {
	return sdb.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", name))
}

// RollbackSavePoint - Rollback a save point
func (sdb *SQLDb) RollbackSavePoint(name string) bool {
	return sdb.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)) && sdb.CommitSavePoint(name)
}

// CreateTable - Create the table definition.
func (sdb *SQLDb) CreateTable(tableDef string) bool {
	return sdb.Exec(fmt.Sprintf("CREATE TABLE %s", tableDef))
}

// CreateIndex - Create the index definition.
func (sdb *SQLDb) CreateIndex(indexDef string) bool {
	return sdb.Exec(fmt.Sprintf("CREATE INDEX %s", indexDef))
}

// Exec - Execute the statement with the bound arguments.
func (sdb *SQLDb) Exec(stmt string, args ...interface{}) bool {
	statement, err := sdb.Prepare(stmt)
	if err != nil {
		log.Printf("DBERROR: Preparing %s: %v", stmt, err)
		return false
	}
	_, err = statement.Exec(args...)
	if err != nil {
		log.Printf("DBERROR: Executing %s: %v", stmt, err)
	}
	return err == nil
}

// SingleQuery - Query the database, and retrieve the results. Expected single value return.
func (sdb *SQLDb) SingleQuery(stmt string, args ...interface{}) bool {
	rows, err := sdb.Query(stmt)
	defer closeRows(rows)
	if err != nil {
		log.Printf("DBERROR: Querying %s: %v", stmt, err)
		return false
	}
	if rows.Next() {
		if args != nil {
			rows.Scan(args...)
		}
		return true
	}
	return false
}

// MultiQuery - Execute a function on the returned query rows.
func (sdb *SQLDb) MultiQuery(stmt string, action func(rows *sql.Rows) bool) bool {
	rows, err := sdb.Query(stmt)
	defer closeRows(rows)
	if err != nil {
		log.Printf("DBERROR: Querying %s: %v", stmt, err)
		return false
	}
	for rows.Next() {
		if !action(rows) {
			return false
		}
	}
	return true
}

func closeRows(rows *sql.Rows) {
	if nil != rows {
		rows.Close()
	}
}
