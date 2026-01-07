package core

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ExecResult captures the outcome of executing a SQL statement.
type ExecResult struct {
	Columns      []string
	Rows         [][]string
	RowsAffected int
	Message      string
}

// Executor runs simple SQL statements against a DB.
type Executor struct {
	db *DB
}

// NewExecutor constructs an Executor.
func NewExecutor(db *DB) *Executor {
	return &Executor{db: db}
}

// Exec executes a single SQL statement.
func (e *Executor) Exec(sql string) (*ExecResult, error) {
	stmt := strings.TrimSpace(sql)
	stmt = strings.TrimSuffix(stmt, ";")
	if stmt == "" {
		return &ExecResult{Message: "empty"}, nil
	}
	upper := strings.ToUpper(stmt)
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		tdef, err := parseCreateTable(stmt)
		if err != nil {
			return nil, err
		}
		if err := e.db.TableNew(tdef); err != nil {
			return nil, err
		}
		return &ExecResult{Message: fmt.Sprintf("table %s created", tdef.Name)}, nil
	case strings.HasPrefix(upper, "INSERT INTO"):
		req, err := e.parseInsert(stmt)
		if err != nil {
			return nil, err
		}
		added, err := e.db.Insert(req.table, req.rec)
		if err != nil {
			return nil, err
		}
		if !added {
			return &ExecResult{Message: "duplicate key", RowsAffected: 0}, nil
		}
		return &ExecResult{Message: "inserted", RowsAffected: 1}, nil
	case strings.HasPrefix(upper, "SELECT"):
		return e.execSelect(stmt)
	default:
		return nil, fmt.Errorf("unsupported statement: %s", stmt)
	}
}

type insertRequest struct {
	table string
	rec   Record
}

func (e *Executor) parseInsert(stmt string) (*insertRequest, error) {
	upper := strings.ToUpper(stmt)
	intoIdx := strings.Index(upper, "INTO")
	if intoIdx < 0 {
		return nil, errors.New("INSERT must contain INTO")
	}
	rest := strings.TrimSpace(stmt[intoIdx+len("INTO"):])
	parts := strings.SplitN(rest, " ", 2)
	if len(parts) < 2 {
		return nil, errors.New("missing table name or columns")
	}
	table := strings.TrimSpace(parts[0])
	colsStart := strings.Index(parts[1], "(")
	valsIdx := strings.Index(strings.ToUpper(parts[1]), "VALUES")
	if colsStart < 0 || valsIdx < 0 {
		return nil, errors.New("INSERT syntax: INSERT INTO tbl (cols) VALUES (vals)")
	}
	colsSection := strings.TrimSpace(parts[1][colsStart:valsIdx])
	valsSection := strings.TrimSpace(parts[1][valsIdx+len("VALUES"):])

	cols, err := parseList(colsSection)
	if err != nil {
		return nil, err
	}
	rawVals, err := parseList(valsSection)
	if err != nil {
		return nil, err
	}
	if len(cols) != len(rawVals) {
		return nil, fmt.Errorf("column count %d does not match values %d", len(cols), len(rawVals))
	}

	tdef := getTableDef(e.db, table)
	if tdef == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	rec := Record{}
	for i, col := range tdef.Cols {
		rec.Cols = append(rec.Cols, col)
		rec.Vals = append(rec.Vals, Value{Type: tdef.Types[i]})
	}

	for i, col := range cols {
		idx := indexOf(tdef.Cols, col)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column: %s", col)
		}
		val, err := parseValue(rawVals[i], tdef.Types[idx])
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col, err)
		}
		rec.Vals[idx] = *val
	}

	if err := valuesComplete(tdef, rec.Vals, len(tdef.Cols)); err != nil {
		return nil, err
	}
	return &insertRequest{table: table, rec: rec}, nil
}

func parseCreateTable(stmt string) (*TableDef, error) {
	namePart := strings.TrimSpace(stmt[len("CREATE TABLE"):])
	openIdx := strings.Index(namePart, "(")
	closeIdx := strings.LastIndex(namePart, ")")
	if openIdx < 0 || closeIdx < 0 || closeIdx <= openIdx {
		return nil, errors.New("invalid CREATE TABLE syntax")
	}
	name := strings.TrimSpace(namePart[:openIdx])
	body := namePart[openIdx : closeIdx+1]
	entries, err := parseList(body)
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, errors.New("table name required")
	}

	cols := make([]string, 0, len(entries))
	types := make([]uint32, 0, len(entries))
	pkCols := []string{}

	for _, entry := range entries {
		up := strings.ToUpper(entry)
		if strings.HasPrefix(up, "PRIMARY KEY") {
			pkCols, err = parseList(entry[len("PRIMARY KEY"):])
			if err != nil {
				return nil, fmt.Errorf("PRIMARY KEY: %w", err)
			}
			continue
		}
		fields := strings.Fields(entry)
		if len(fields) < 2 {
			return nil, fmt.Errorf("invalid column definition: %s", entry)
		}
		col := fields[0]
		typ, err := parseType(fields[1])
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		types = append(types, typ)
		for i := 2; i < len(fields)-1; i++ {
			if strings.EqualFold(fields[i], "primary") && strings.EqualFold(fields[i+1], "key") {
				pkCols = append(pkCols, col)
				break
			}
		}
	}

	if len(pkCols) == 0 {
		return nil, errors.New("PRIMARY KEY required")
	}

	orderedCols := make([]string, 0, len(cols))
	orderedTypes := make([]uint32, 0, len(cols))
	for _, pk := range pkCols {
		idx := indexOf(cols, pk)
		if idx < 0 {
			return nil, fmt.Errorf("primary key column %s not defined", pk)
		}
		orderedCols = append(orderedCols, cols[idx])
		orderedTypes = append(orderedTypes, types[idx])
	}
	for i, col := range cols {
		if indexOf(pkCols, col) >= 0 {
			continue
		}
		orderedCols = append(orderedCols, col)
		orderedTypes = append(orderedTypes, types[i])
	}

	return &TableDef{
		Name:  name,
		Cols:  orderedCols,
		Types: orderedTypes,
		PKeys: len(pkCols),
	}, nil
}

func parseType(name string) (uint32, error) {
	switch strings.ToUpper(name) {
	case "INT", "INT64":
		return TypeInt64, nil
	case "TEXT", "STRING", "BYTES":
		return TypeBytes, nil
	default:
		return 0, fmt.Errorf("unsupported type: %s", name)
	}
}

func parseValue(raw string, typ uint32) (*Value, error) {
	raw = strings.TrimSpace(raw)
	switch typ {
	case TypeInt64:
		i, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int64: %s", raw)
		}
		return &Value{Type: TypeInt64, I64: i}, nil
	case TypeBytes:
		if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
			unquoted := strings.Trim(raw, "'")
			unquoted = strings.ReplaceAll(unquoted, "''", "'")
			return &Value{Type: TypeBytes, Str: []byte(unquoted)}, nil
		}
		return &Value{Type: TypeBytes, Str: []byte(raw)}, nil
	default:
		return nil, fmt.Errorf("unknown type: %d", typ)
	}
}

func (e *Executor) execSelect(stmt string) (*ExecResult, error) {
	upper := strings.ToUpper(stmt)
	selectIdx := len("SELECT")
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return nil, errors.New("SELECT missing FROM")
	}
	colsSection := strings.TrimSpace(stmt[selectIdx:fromIdx])
	remainder := strings.TrimSpace(stmt[fromIdx+len("FROM"):])

	parts := strings.Fields(remainder)
	if len(parts) == 0 {
		return nil, errors.New("missing table name")
	}
	table := parts[0]

	whereClause := strings.TrimSpace(strings.TrimPrefix(remainder[len(table):], ""))
	where := map[string]string{}
	if widx := strings.Index(strings.ToUpper(whereClause), "WHERE"); widx >= 0 {
		cond := strings.TrimSpace(whereClause[widx+len("WHERE"):])
		if cond == "" {
			return nil, errors.New("WHERE clause cannot be empty")
		}
		pairs := strings.Split(cond, "AND")
		for _, p := range pairs {
			kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
			if len(kv) != 2 {
				return nil, fmt.Errorf("invalid WHERE condition: %s", p)
			}
			where[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}

	tdef := getTableDef(e.db, table)
	if tdef == nil {
		return nil, fmt.Errorf("table not found: %s", table)
	}

	var selectedCols []string
	colsList, err := parseList(colsSection)
	if err != nil {
		return nil, err
	}
	if len(colsList) == 1 && colsList[0] == "*" {
		selectedCols = tdef.Cols
	} else {
		for _, c := range colsList {
			if indexOf(tdef.Cols, c) < 0 {
				return nil, fmt.Errorf("unknown column: %s", c)
			}
			selectedCols = append(selectedCols, c)
		}
	}

	pkVals := make([]Value, tdef.PKeys)
	keysProvided := 0
	for i := 0; i < tdef.PKeys; i++ {
		col := tdef.Cols[i]
		raw, ok := where[col]
		if !ok {
			break
		}
		val, err := parseValue(raw, tdef.Types[i])
		if err != nil {
			return nil, err
		}
		pkVals[i] = *val
		keysProvided++
	}

	rows := [][]string{}
	if keysProvided == tdef.PKeys {
		rec := Record{}
		for i := 0; i < tdef.PKeys; i++ {
			switch pkVals[i].Type {
			case TypeInt64:
				rec.AddInt64(tdef.Cols[i], pkVals[i].I64)
			case TypeBytes:
				rec.AddStr(tdef.Cols[i], pkVals[i].Str)
			}
		}
		ok, err := e.db.Get(table, &rec)
		if err != nil {
			return nil, err
		}
		if ok {
			rows = append(rows, projectRow(&rec, selectedCols))
		}
	} else {
		minRec, maxRec := buildBounds(tdef)
		sc := Scanner{Cmp1: CmpGe, Cmp2: CmpLe, Key1: *minRec, Key2: *maxRec}
		if err := e.db.Scan(table, &sc); err != nil {
			return nil, err
		}
		cur := Record{}
		for sc.Valid() {
			sc.Deref(&cur)
			match := true
			for col, raw := range where {
				idx := indexOf(tdef.Cols, col)
				if idx < 0 {
					return nil, fmt.Errorf("unknown column: %s", col)
				}
				expected, err := parseValue(raw, tdef.Types[idx])
				if err != nil {
					return nil, err
				}
				if !valueEquals(cur.Vals[idx], *expected) {
					match = false
					break
				}
			}
			if match {
				rows = append(rows, projectRow(&cur, selectedCols))
			}
			sc.Next()
		}
	}

	return &ExecResult{Columns: selectedCols, Rows: rows, RowsAffected: len(rows)}, nil
}

func buildBounds(tdef *TableDef) (*Record, *Record) {
	minRec := &Record{}
	maxRec := &Record{}
	for i := 0; i < tdef.PKeys; i++ {
		switch tdef.Types[i] {
		case TypeInt64:
			minRec.AddInt64(tdef.Cols[i], math.MinInt64)
			maxRec.AddInt64(tdef.Cols[i], math.MaxInt64)
		case TypeBytes:
			minRec.AddStr(tdef.Cols[i], []byte{})
			maxRec.AddStr(tdef.Cols[i], bytes.Repeat([]byte{0xFF}, 8))
		default:
			panic("unsupported type")
		}
	}
	return minRec, maxRec
}

func projectRow(rec *Record, cols []string) []string {
	out := make([]string, len(cols))
	for i, col := range cols {
		v := rec.Get(col)
		if v == nil {
			out[i] = ""
			continue
		}
		switch v.Type {
		case TypeInt64:
			out[i] = fmt.Sprintf("%d", v.I64)
		case TypeBytes:
			out[i] = string(v.Str)
		}
	}
	return out
}

func valueEquals(a, b Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case TypeInt64:
		return a.I64 == b.I64
	case TypeBytes:
		return bytes.Equal(a.Str, b.Str)
	default:
		return false
	}
}

func parseList(section string) ([]string, error) {
	section = strings.TrimSpace(section)
	if strings.HasPrefix(section, "(") {
		if !strings.HasSuffix(section, ")") {
			return nil, errors.New("missing closing parenthesis")
		}
		section = section[1 : len(section)-1]
	}
	var parts []string
	cur := strings.Builder{}
	inQuote := false
	for i := 0; i < len(section); i++ {
		ch := section[i]
		switch ch {
		case '\'':
			if inQuote && i+1 < len(section) && section[i+1] == '\'' {
				cur.WriteByte('\'')
				i++
			} else {
				inQuote = !inQuote
			}
		case ',':
			if inQuote {
				cur.WriteByte(ch)
			} else {
				part := strings.TrimSpace(cur.String())
				if part != "" {
					parts = append(parts, part)
				}
				cur.Reset()
			}
		default:
			cur.WriteByte(ch)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, strings.TrimSpace(cur.String()))
	}
	if len(parts) == 0 {
		return nil, errors.New("empty list")
	}
	return parts, nil
}

func indexOf(slice []string, target string) int {
	for i, v := range slice {
		if strings.EqualFold(v, target) {
			return i
		}
	}
	return -1
}
