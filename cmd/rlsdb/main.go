package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"db-practice/core"
)

func main() {
	dbPath := flag.String("db", "rls.db", "path to database file")
	flag.Parse()

	db := core.DB{Path: *dbPath}
	if err := db.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	exec := core.NewExecutor(&db)
	reader := bufio.NewScanner(os.Stdin)
	fmt.Printf("RLSDB ready. Using %s\n", *dbPath)
	fmt.Print("rlsdb> ")
	for reader.Scan() {
		line := strings.TrimSpace(reader.Text())
		if strings.EqualFold(line, "quit") || strings.EqualFold(line, "exit") {
			break
		}
		if line == "" {
			fmt.Print("rlsdb> ")
			continue
		}
		res, err := exec.Exec(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		if len(res.Columns) > 0 {
			renderTable(res)
		}
		if res.Message != "" {
			fmt.Println(res.Message)
		}
		if res.RowsAffected > 0 && res.Message == "" {
			fmt.Printf("%d rows\n", res.RowsAffected)
		}
		fmt.Print("rlsdb> ")
	}
	if err := reader.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("bye")
}

func renderTable(res *core.ExecResult) {
	widths := make([]int, len(res.Columns))
	for i, c := range res.Columns {
		widths[i] = len(c)
	}
	for _, row := range res.Rows {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	printRow := func(cells []string) {
		for i, cell := range cells {
			fmt.Printf("%-*s", widths[i]+2, cell)
		}
		fmt.Println()
	}
	printRow(res.Columns)
	for _, row := range res.Rows {
		printRow(row)
	}
}
