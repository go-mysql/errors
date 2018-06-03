package errors_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	my "github.com/go-mysql/errors"
	_ "github.com/go-sql-driver/mysql"
)

// default_connection.json in sandbox dir
type connInfo struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	Socket string `json:"socket"`
	User   string `json:"username"`
	Pass   string `json:"password"`
}

var (
	sandboxDir  string
	defaultDSN  string
	defaultPort string
)

// Runs a script in sandbox dir
func sandboxAction(t *testing.T, action string) {
	cmd := exec.Command(filepath.Join(sandboxDir, action))
	t.Logf("%s sandbox", action)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Log(out)
		t.Fatal(err)
	}
}

func setup(t *testing.T) {
	if sandboxDir == "" || defaultDSN == "" {
		sandboxDir = os.Getenv("MYSQL_SANDBOX_DIR")
		if sandboxDir == "" {
			t.Fatal("MYSQL_SANDBOX_DIR is not set")
		}
		t.Logf("sandbox dir: %s", sandboxDir)

		bytes, err := ioutil.ReadFile(filepath.Join(sandboxDir, "default_connection.json"))
		if err != nil {
			t.Fatalf("cannot read MYSQL_SANDBOX_DIR/default_connection.json: %s", err)
		}

		var c connInfo
		if err := json.Unmarshal(bytes, &c); err != nil {
			t.Fatalf("cannot unmarshal MYSQL_SANDBOX_DIR/default_connection.json: %s", err)
		}

		defaultPort = c.Port
		defaultDSN = fmt.Sprintf("msandbox:%s@tcp(%s:%s)/", c.Pass, c.Host, c.Port)
		t.Logf("dsn: %s", defaultDSN)
	}
	sandboxAction(t, "start")
}

func newDB(t *testing.T, dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("cannot connect to %s: %s", dsn, err)
	}
	return db
}

func TestLostConnection(t *testing.T) {
	setup(t)

	// Create the normal DB but also...
	db := newDB(t, defaultDSN)

	// Create a DB for connections to kill conns in the normal DB
	kill := newDB(t, defaultDSN)

	// The normal pool should have only 1 conn, get its ID so we
	// can kill it to simulate a lost connection
	ctx := context.TODO()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Get conn ID for the KILL query, then close the conn (doesn't actually
	// close the network conn, it just releases the conn back into the pool).
	var id string
	err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("conn id: %s", id)
	conn.Close()

	// Kill the conn
	k, err := kill.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = k.ExecContext(ctx, "KILL "+id)
	if err != nil {
		t.Fatal(err)
	}

	// Try to use the normal DB conn again and it should throw errors.
	// Because this is a sql.Conn, the driver is going to throw "packets.go:36:
	// unexpected EOF" first, then "connection.go:373: invalid connection" second,
	// then finally reconnect the conn. Don't ask me why; that's just the
	// behavior I observe from go-sql-driver/mysql v1.3. Ideally, it should
	// throw only one error when it first finds the conn is invalid, as sql.DB does.
	recovered := false
	for i := 0; i < 3; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
		conn.Close()
		if err != nil {
			// Error() should report this as a conn issue (lost=true) and the
			// error should be ErrConnLost which is a human-friendly wrapper
			// for various lower-level errors that mean the same.
			ok, myerr := my.Error(err)
			t.Logf("try %d: %s", i, err)
			if !ok {
				t.Errorf("MySQL error = false, expected true (try %d)", i)
			}
			if myerr != my.ErrConnLost {
				t.Errorf("got error '%v', expected ErrConnLost", err)
			}
		} else {
			recovered = true
			break
		}
	}
	if !recovered {
		t.Error("pool conn did not recover after 3 retries")
	}
}

func TestMySQLDown(t *testing.T) {
	setup(t)

	// Make one connection as usual and get its ID to verify it's ok
	db := newDB(t, defaultDSN)
	ctx := context.TODO()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var id string
	err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	t.Logf("conn id: %s", id)

	// Stop MySQL sandbox
	sandboxAction(t, "stop")

	// As far as db knows, its connectinos are still valid, but try to use one
	// and it'll throw two ErrConnLost before it tries to reconnect on db.Conn()
	// which will throw ErrErrConnCannotConnect.
	for i := 0; i < 2; i++ {
		conn, err = db.Conn(ctx) // no error yet
		if err != nil {
			t.Fatal(err)
		}
		err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
		conn.Close()
		if err == nil {
			t.Error("got nil error, expected ErrConnLost")
		} else {
			ok, myerr := my.Error(err)
			t.Logf("try %d: %s", i, err)
			if !ok {
				t.Errorf("MySQL error = false, expected true (try %d)", i)
			}
			if myerr != my.ErrConnLost {
				t.Errorf("got error '%v', expected ErrConnLost", err)
			}
		}
	}

	// Now db.Conn() starts throwing ErrCannotConnect
	for i := 2; i < 4; i++ {
		conn, err = db.Conn(ctx)
		if err == nil {
			t.Error("got nil error, expected ErrCannotConnect")
		} else {
			ok, myerr := my.Error(err)
			t.Logf("try %d: %s", i, err)
			if !ok {
				t.Errorf("MySQL error = false, expected true (try %d)", i)
			}
			if myerr != my.ErrCannotConnect {
				t.Errorf("got error '%v', expected ErrCannotConnect", err)
			}
		}
	}
}

func TestServerShutdown(t *testing.T) {
	// MySQL is so polite that it'll tell you when it's shutting down.
	setup(t)

	// Make one connection as usual
	db := newDB(t, defaultDSN)
	ctx := context.TODO()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}

	// Fake a long-running query so we can shutdown MySQL while it's running
	doneChan := make(chan error, 1)
	go func() {
		defer conn.Close()
		var n string
		err1 := conn.QueryRowContext(ctx, "SELECT SLEEP(5) FROM mysql.user").Scan(&n)
		doneChan <- err1
	}()

	// Stop MySQL sandbox
	time.Sleep(500 * time.Millisecond) // yield to goroutine
	sandboxAction(t, "stop")

	// The goroutine shoud return immediately with "Error 1053: Server shutdown in progress",
	// which we treat as ErrConnLost because the conn is about to be lost.
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SELECT SLEEP(5) to return")
	case err = <-doneChan:
		t.Logf("recv err: %v", err)
		ok, myerr := my.Error(err)
		if !ok {
			t.Error("MySQL error = false, expected true)")
		}
		if myerr != my.ErrConnLost {
			t.Errorf("got error '%v', expected ErrConnLost", err)
		}
	}
}

func TestKillQuery(t *testing.T) {
	setup(t)

	// Like TestLostConnection we'll need a normal and a kill db
	db := newDB(t, defaultDSN)
	kill := newDB(t, defaultDSN)

	// The normal db should have only 1 conn, get its ID so we
	// can kill it to simulate a lost connection
	ctx := context.TODO()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var id string
	err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	t.Logf("conn id: %s", id)

	// Like TestServerShutdown simulate a long-running query we can kill
	doneChan := make(chan error, 1)
	go func() {
		conn, err = db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var n string
		err1 := conn.QueryRowContext(ctx, "SELECT SLEEP(5) FROM mysql.user").Scan(&n)
		doneChan <- err1
	}()
	time.Sleep(500 * time.Millisecond) // yield to goroutine

	// Kill the conn in the normal db
	k, err := kill.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = k.ExecContext(ctx, "KILL QUERY "+id)
	if err != nil {
		t.Fatal(err)
	}

	// The goroutine should return immediately because we killed the query, which
	// causes "Error 1317: Query execution was interrupted" aka ErrQueryKilled
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SELECT SLEEP(5) to return")
	case err = <-doneChan:
		t.Logf("recv err: %v", err)
		ok, myerr := my.Error(err)
		if !ok {
			t.Error("MySQL error = false, expected true)")
		}
		if myerr != my.ErrQueryKilled {
			t.Errorf("got error '%v', expected ErrQueryKilled", err)
		}
	}
}

func TestReadOnly(t *testing.T) {
	setup(t)

	// Make a connection as usual
	db := newDB(t, defaultDSN)
	ctx := context.TODO()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("got nil conn, expected it to be set")
	}
	defer conn.Close()

	// Make MySQL read-only
	_, err = conn.ExecContext(ctx, "SET GLOBAL read_only=1, super_read_only=1")
	if err != nil {
		t.Fatal(err)
	}

	// Make MySQL writeable again when done
	defer func() {
		_, err = conn.ExecContext(ctx, "SET GLOBAL read_only=0, super_read_only=0")
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Try to write something, which should cause "Error 1290: The MySQL server
	// is running with the --read-only option so it cannot execute this statement"
	_, err = conn.ExecContext(ctx, "CREATE TABLE test.t (id INT)")
	t.Logf("err: %v", err)
	ok, myerr := my.Error(err)
	if !ok {
		t.Error("MySQL error = false, expected true)")
	}
	if myerr != my.ErrReadOnly {
		t.Errorf("got error '%v', expected ErrReadOnly", err)
	}
}

func TestDupeKey(t *testing.T) {
	setup(t)

	// Make a connection as usual
	db := newDB(t, defaultDSN)
	ctx := context.TODO()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	if conn == nil {
		t.Fatal("got nil conn, expected it to be set")
	}
	defer conn.Close()

	// Make a test table and insert a value
	queries := []string{
		"DROP TABLE IF EXISTS test.t",
		"CREATE TABLE test.t (id INT, UNIQUE KEY (id))",
		"INSERT INTO test.t VALUES (1)",
	}
	for _, q := range queries {
		_, err := conn.ExecContext(ctx, q)
		if err != nil {
			t.Fatalf("%s: %s", q, err)
		}
	}
	defer func() {
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS test.t")
		if err != nil {
			t.Errorf("cannot drop table test.t: %s", err)
		}
	}()

	// Insert a duplicate value which causes "Error 1062: Duplicate entry '1' for key 'id'"
	_, err = conn.ExecContext(ctx, "INSERT INTO test.t VALUES (1)")
	t.Logf("err: %v", err)
	ok, myerr := my.Error(err)
	if !ok {
		t.Error("MySQL error = false, expected true)")
	}
	if myerr != my.ErrDupeKey {
		t.Errorf("got error '%v', expected ErrDupeKey", err)
	}
}

func TestRandomError(t *testing.T) {
	setup(t)

	// Make a connection as usual
	db := newDB(t, defaultDSN)
	ctx := context.TODO()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("got pool.Open error: %s, expected nil", err)
	}
	if conn == nil {
		t.Fatal("got nil conn, expected it to be set")
	}
	defer conn.Close()

	// It's easy to produce a random error we don't handle.
	// Example: syntax error: "Error 1064: You have an error in your SQL syntax; ..."
	_, err = conn.ExecContext(ctx, "INSERT bananas!")
	t.Logf("err: %v", err)
	ok, err := my.Error(err)
	if ok != true {
		t.Error("MySQL error = false, expected true")
	}
	if err == nil {
		t.Errorf("got nil err, expected an error")
	}

	// While we're here, let's test MySQLErrorCode
	errCode := my.MySQLErrorCode(err)
	if errCode != 1064 {
		t.Errorf("got MySQL error code %d, expected 1064", errCode)
	}
}

func TestNotMySQLError(t *testing.T) {
	notMySQLErr := fmt.Errorf("not a mysql error")

	ok, myerr := my.Error(notMySQLErr)
	if ok == true {
		t.Error("MySQL error = true, expected false")
	}
	if myerr != notMySQLErr {
		t.Errorf("got err '%s', exected '%s'", myerr, notMySQLErr)
	}
	ok, myerr = my.Error(nil)
	if ok == true {
		t.Error("MySQL error = true, expected false")
	}
	if myerr != nil {
		t.Errorf("got err '%s', exected '%s'", myerr, notMySQLErr)
	}

	errcode := my.MySQLErrorCode(notMySQLErr)
	if errcode != 0 {
		t.Errorf("got MySQL error code %d, expected 0", errcode)
	}
	errcode = my.MySQLErrorCode(nil)
	if errcode != 0 {
		t.Errorf("got MySQL error code %d, expected 0", errcode)
	}
}

func TestCanRetry(t *testing.T) {
	notMySQLErr := fmt.Errorf("not a mysql error")

	// CanRetry = false
	retry := my.CanRetry(notMySQLErr)
	if retry == true {
		t.Error("can try = true, expected false")
	}
	retry = my.CanRetry(nil)
	if retry == true {
		t.Error("can try = true, expected false")
	}
	retry = my.CanRetry(my.ErrDupeKey)
	if retry == true {
		t.Error("can try = true, expected false")
	}

	// CanRetry = true
	retry = my.CanRetry(my.ErrCannotConnect)
	if retry == false {
		t.Error("can try = false, expected true")
	}
	retry = my.CanRetry(my.ErrConnLost)
	if retry == false {
		t.Error("can try = false, expected true")
	}
	retry = my.CanRetry(my.ErrQueryKilled)
	if retry == false {
		t.Error("can try = false, expected true")
	}
	retry = my.CanRetry(my.ErrReadOnly)
	if retry == false {
		t.Error("can try = false, expected true")
	}
}
