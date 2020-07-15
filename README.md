# mysql-go
Golang MySQL Library

# Install
`go get -u github.com/vinhjaxt/mysql-go`

# Usage
`import "github.com/vinhjaxt/mysql-go"`

# API:
- db.Insert, db.Update, db.InsertUpdate, db.Delete, db.Query, db.Single, db.Row, db.Rows
- db.SetRows select rows of each result sets excludes nil set
- db.SetRowsNil select rows of each result sets includes nil set
- db.Escape, db.EscapeID
