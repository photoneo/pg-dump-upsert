package pgdump

import (
	"bytes"
)

// getQueryStatement returns SELECT statement to retrieve rows to dump.
func getQueryStatement(table string, cols []column) string {
	var buf bytes.Buffer
	buf.WriteString("SELECT ")

	count := 0

	for _, col := range cols {
		if col.insert {
			if count > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(quoteIdentifier(col.Name))
			count++
		}
	}

	buf.WriteString(" FROM " + quoteIdentifier(table))

	return buf.String()
}

// getInsertStatement returns INSERT statement to output for row currently
// loaded in to cols slice.
func getInsertStatement(table string, cols []column, opts *Options) string {
	var buf bytes.Buffer

	buf.WriteString("INSERT INTO " + quoteIdentifier(table) + " (")
	count := 0

	for _, col := range cols {
		if col.insert {
			if count > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(quoteIdentifier(col.Name))
			count++
		}
	}

	buf.WriteString(") VALUES (")
	count = 0

	for _, col := range cols {
		if col.insert {
			if count > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.literal())
			count++
		}
	}

	buf.WriteString(")")

	if opts.ConflictColumn != "" {
		buf.WriteString(" ON CONFLICT (" + opts.ConflictColumn + ") DO UPDATE SET ")
		count = 0
		for _, col := range cols {
			if col.update {
				if count > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(quoteIdentifier(col.Name) + "=EXCLUDED." + quoteIdentifier(col.Name))
				count++
			}
		}
	} else if opts.NoConflict {
		buf.WriteString(" ON CONFLICT DO NOTHING")
	}

	buf.WriteString(";\n")

	return buf.String()
}

func quoteIdentifier(col string) string {
	return "\"" + col + "\""
}
