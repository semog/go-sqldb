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
	PatchFunc func(sdb *SQLDb) error
}

// The array of patch functions that will automatically upgrade the database.
// Internal patch IDs are reserved to be zero or negative. User patch IDs are positive ints.
var internalPatchDbFuncs = []PatchFuncType{
	{0, func(sdb *SQLDb) error {
		return sdb.CreateTable("IF NOT EXISTS version (patchid INTEGER PRIMARY KEY)")
	}},
	{-1, func(sdb *SQLDb) error {
		if err := sdb.CreateTable("IF NOT EXISTS gkey (next INTEGER PRIMARY KEY)"); err != nil {
			return nil
		}
		// Insert initial value of 1 into the gkey table
		return sdb.Exec("INSERT INTO gkey (next) VALUES (1)")
	}},
}

// OpenAndPatchDb - Open and Patch a database if necessary.
func OpenAndPatchDb(dbFilename string, patchFuncs []PatchFuncType) (*SQLDb, error) {
	sdb, err := OpenDb(dbFilename)
	if err != nil {
		return sdb, err
	}
	if err := sdb.PatchDb(patchFuncs); err != nil {
		return sdb, err
	}
	return sdb, nil
}

// OpenDb - Open a database.
func OpenDb(dbFilename string) (*SQLDb, error) {
	var err error
	sdb := &SQLDb{}
	sdb.DB, err = sql.Open("sqlite3", dbFilename)
	if err != nil {
		return sdb, err
	}
	if nil != sdb.DB.Ping() {
		return sdb, fmt.Errorf("could not communicate with database: %s", dbFilename)
	}
	return sdb, nil
}

// PatchDb - Patch a database if necessary.
func (sdb *SQLDb) PatchDb(patchFuncs []PatchFuncType) error {
	// Always run internal patch functions first
	if err := sdb.patch(internalPatchDbFuncs); err != nil {
		return err
	}
	if patchFuncs == nil {
		// User does not want to do their own patching
		return nil
	}
	// Run the user patches
	return sdb.patch(patchFuncs)
}

func (sdb *SQLDb) patch(patchFuncs []PatchFuncType) error {
	// Currently this patching function does not check to see when it is
	// finished whether it is running against a _newer_ database. An additional
	// check would need to be done to see if the final committed patchid matches the
	// expected patchid.
	for _, patch := range patchFuncs {
		if !sdb.patched(patch.PatchID) {
			if err := sdb.beginPatch(); err != nil {
				return fmt.Errorf("could not begin patch database for version %d: %v", patch.PatchID, err)
			}
			if err := patch.PatchFunc(sdb); err != nil {
				sdb.rollbackPatch()
				return fmt.Errorf("could not patch database for version %d: %v", patch.PatchID, err)
			}
			if err := sdb.commitPatch(patch.PatchID); err != nil {
				sdb.rollbackPatch()
				return fmt.Errorf("could not commit patch database for version %d: %v", patch.PatchID, err)
			}
		}
	}
	return nil
}

// GetGkey - Get a gkey to be used as unique record ID
func (sdb *SQLDb) GetGkey() (int, error) {
	// Read next value from gkey table. Increment gkey table next value.
	if err := sdb.BeginTrans(); err != nil {
		return 0, err
	}

	var gkey int
	if err := sdb.SingleQuery("SELECT next FROM gkey", &gkey); err != nil {
		sdb.RollbackTrans()
		return 0, err
	}

	if err := sdb.Exec("UPDATE gkey SET next = ? WHERE next = ?", gkey+1, gkey); err != nil {
		sdb.RollbackTrans()
		return 0, err
	}

	return gkey, sdb.CommitTrans()
}

// BeginTrans - Begin transaction
func (sdb *SQLDb) BeginTrans() error {
	return sdb.Exec("BEGIN")
}

// CommitTrans - Commit transaction
func (sdb *SQLDb) CommitTrans() error {
	return sdb.Exec("COMMIT")
}

// RollbackTrans - Rollback transaction
func (sdb *SQLDb) RollbackTrans() error {
	return sdb.Exec("ROLLBACK")
}

// CommitOnSuccess - Commit the transaction if the expression evaluates to true.
func (sdb *SQLDb) CommitOnSuccess(success bool) error {
	if success {
		return sdb.CommitTrans()
	}
	return sdb.RollbackTrans()
}

// CommitOnNoError - Commit the transaction if the error is nil
func (sdb *SQLDb) CommitOnNoError(err error) error {
	if err != nil {
		if rberr := sdb.RollbackTrans(); rberr != nil {
			log.Print(rberr)
		}
		return err
	}
	return sdb.CommitTrans()
}

// CommitSavePointOnSuccess - Commit up to the save point (or merge with parent transaction) if the expression evaluates to true.
func (sdb *SQLDb) CommitSavePointOnSuccess(name string, success bool) error {
	if success {
		return sdb.CommitSavePoint(name)
	}
	return sdb.RollbackSavePoint(name)
}

// CommitSavePointOnNoError - Commit up to the save point (or merge with parent transaction) if the error is nil.
func (sdb *SQLDb) CommitSavePointOnNoError(name string, err error) error {
	if err != nil {
		if rberr := sdb.RollbackSavePoint(name); rberr != nil {
			log.Print(rberr)
		}
		return err
	}
	return sdb.CommitSavePoint(name)
}

// ExecWithSavePoint - Execute the database function wrapped inside of a named Save Point.
func (sdb *SQLDb) ExecWithSavePoint(spName string, fn func() error) error {
	if err := sdb.CreateSavePoint(spName); err != nil {
		return err
	}
	// Commit if the function has no errors
	return sdb.CommitSavePointOnNoError(spName, fn())
}

func (sdb *SQLDb) patched(patchid int) bool {
	// Check for the patchid in the version table
	return nil == sdb.SingleQuery(fmt.Sprintf("SELECT patchid FROM version WHERE patchid = %d", patchid))
}

func (sdb *SQLDb) beginPatch() error {
	return sdb.CreateSavePoint(patchSavePointName)
}

func (sdb *SQLDb) commitPatch(patchid int) error {
	// Add the patchid to the versions table. If it fails, return false.
	if err := sdb.Exec(fmt.Sprintf("INSERT OR FAIL INTO version (patchid) VALUES (%d)", patchid)); err != nil {
		return err
	}
	return sdb.CommitSavePoint(patchSavePointName)
}

func (sdb *SQLDb) rollbackPatch() {
	sdb.RollbackTrans()
}

// CreateSavePoint - Create a save point for rollback or commit.
func (sdb *SQLDb) CreateSavePoint(name string) error {
	return sdb.Exec(fmt.Sprintf("SAVEPOINT %s", name))
}

// CommitSavePoint - Commit up to the named save point, which rolls it up into parent transaction.
func (sdb *SQLDb) CommitSavePoint(name string) error {
	return sdb.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", name))
}

// RollbackSavePoint - Rollback a save point
func (sdb *SQLDb) RollbackSavePoint(name string) error {
	if err := sdb.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)); err != nil {
		return err
	}
	return sdb.CommitSavePoint(name)
}

// CreateTable - Create the table definition.
func (sdb *SQLDb) CreateTable(tableDef string) error {
	return sdb.Exec(fmt.Sprintf("CREATE TABLE %s", tableDef))
}

// DropTable - Drop the table definition.
func (sdb *SQLDb) DropTable(tableDef string) error {
	return sdb.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableDef))
}

// CreateIndex - Create the index definition.
func (sdb *SQLDb) CreateIndex(indexDef string) error {
	return sdb.Exec(fmt.Sprintf("CREATE INDEX %s", indexDef))
}

// ExecResults - Execute the statement with the bound arguments.
func (sdb *SQLDb) ExecResults(stmt string, args ...interface{}) (sql.Result, error) {
	statement, err := sdb.Prepare(stmt)
	defer closeStmt(statement)
	if err != nil {
		return nil, fmt.Errorf("dberror: preparing %s: %v", stmt, err)
	}
	var res sql.Result
	res, err = statement.Exec(args...)
	if err != nil {
		return nil, fmt.Errorf("dberror: executing %s: %v", stmt, err)
	}
	return res, nil
}

// Exec - Execute the statement with the bound arguments.
func (sdb *SQLDb) Exec(stmt string, args ...interface{}) error {
	_, err := sdb.ExecResults(stmt, args...)
	return err
}

// SingleQuery - Query the database, and retrieve the results. Expected single value return.
func (sdb *SQLDb) SingleQuery(stmt string, args ...interface{}) error {
	rows, err := sdb.Query(stmt)
	defer closeRows(rows)
	if err != nil {
		return fmt.Errorf("dberror: querying %s: %v", stmt, err)
	}
	if rows.Next() {
		if args != nil {
			return rows.Scan(args...)
		}
		return nil
	}
	return fmt.Errorf("dberror: could not retrieve query value for %s", stmt)
}

// MultiQuery - Execute a function on the returned query rows.
func (sdb *SQLDb) MultiQuery(stmt string, action func(rows *sql.Rows) error) error {
	rows, err := sdb.Query(stmt)
	defer closeRows(rows)
	if err != nil {
		return fmt.Errorf("dberror: querying %s: %v", stmt, err)
	}
	for rows.Next() {
		if err := action(rows); err != nil {
			return err
		}
	}
	return nil
}

func closeStmt(stmt *sql.Stmt) {
	if nil != stmt {
		stmt.Close()
	}
}

func closeRows(rows *sql.Rows) {
	if nil != rows {
		rows.Close()
	}
}
