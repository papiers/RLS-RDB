package core

import (
	"os"
	"testing"

	is "github.com/stretchr/testify/require"
)

func TestExecutorEndToEnd(t *testing.T) {
	path := "exec_test.db"
	_ = os.Remove(path)
	db := DB{Path: path}
	err := db.Open()
	is.NoError(t, err)
	defer func() {
		db.Close()
		_ = os.Remove(path)
	}()

	exec := NewExecutor(&db)
	res, err := exec.Exec("CREATE TABLE users(id INT PRIMARY KEY, name TEXT);")
	is.NoError(t, err)
	is.Contains(t, res.Message, "users")

	res, err = exec.Exec("INSERT INTO users (id, name) VALUES (1, 'alice');")
	is.NoError(t, err)
	is.Equal(t, 1, res.RowsAffected)

	res, err = exec.Exec("INSERT INTO users (id, name) VALUES (2, 'bob');")
	is.NoError(t, err)
	is.Equal(t, 1, res.RowsAffected)

	res, err = exec.Exec("SELECT id, name FROM users;")
	is.NoError(t, err)
	is.Equal(t, []string{"id", "name"}, res.Columns)
	is.Equal(t, 2, len(res.Rows))
	is.Equal(t, []string{"1", "alice"}, res.Rows[0])

	res, err = exec.Exec("SELECT name FROM users WHERE id = 2;")
	is.NoError(t, err)
	is.Equal(t, []string{"name"}, res.Columns)
	is.Equal(t, [][]string{{"bob"}}, res.Rows)
}
