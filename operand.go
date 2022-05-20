package gotlin

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/spf13/cast"
	"github.com/vmihailenco/msgpack/v5"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var EmptyQueryResult = struct{}{}

type Operand struct {
	Type  string
	Value OperandValuer
}

func NewEmptyOperand() Operand {
	v := EmptyInput{}
	return Operand{Type: v.Type(), Value: v}
}

func NewImmediateValue(u interface{}) Operand {
	v := Immediate{Value: u}
	return Operand{Type: v.Type(), Value: v}
}

func NewDatabaseInputOperand(v DatabaseInput) Operand {
	return Operand{Type: v.Type(), Value: v}
}

func (v Operand) OperandValue(ctx context.Context) (interface{}, error) {
	return v.Value.OperandValue(ctx)
}

func (v *Operand) ImmediateValue() (interface{}, bool) {
	u, ok := v.Value.(Immediate)
	if !ok {
		return nil, false
	}
	return u.Value, true
}

func (v *Operand) UnmarshalJSON(data []byte) (err error) {
	return v.unmarshalJSON(data, json.Unmarshal)
}

func (v *Operand) unmarshalJSON(data []byte, f Unmarshal) (err error) {
	root := struct {
		Type  string
		Value json.RawMessage
	}{}

	err = f(data, &root)
	if err != nil {
		return
	}

	var (
		emptyInput    = EmptyInput{}
		immediate     = Immediate{}
		databaseQuery = DatabaseInput{}
	)

	switch root.Type {
	case emptyInput.Type():
		err = f(root.Value, &emptyInput)
		*v = Operand{Type: root.Type, Value: emptyInput}
	case immediate.Type():
		err = f(root.Value, &immediate)
		*v = Operand{Type: root.Type, Value: immediate}
	case databaseQuery.Type():
		err = f(root.Value, &databaseQuery)
		*v = Operand{Type: root.Type, Value: databaseQuery}
	default:
		return newErrorf("Unmarshal Operand for type %s", root.Type)
	}
	return
}

func (v *Operand) UnmarshalMsgpack(data []byte) error {
	return v.unmarshalMsgpack(data, msgpack.Unmarshal)
}

func (v *Operand) unmarshalMsgpack(data []byte, f Unmarshal) (err error) {
	root := struct {
		Type  string
		Value msgpack.RawMessage
	}{}

	err = f(data, &root)
	if err != nil {
		return
	}

	var (
		emptyInput    = EmptyInput{}
		immediate     = Immediate{}
		databaseQuery = DatabaseInput{}
	)

	switch root.Type {
	case emptyInput.Type():
		err = f(root.Value, &emptyInput)
		*v = Operand{Type: root.Type, Value: emptyInput}
	case immediate.Type():
		err = f(root.Value, &immediate)
		*v = Operand{Type: root.Type, Value: immediate}
	case databaseQuery.Type():
		err = f(root.Value, &databaseQuery)
		*v = Operand{Type: root.Type, Value: databaseQuery}
	default:
		return newErrorf("Unmarshal Operand for type %s", root.Type)
	}
	return
}

type OperandValuer interface {
	Type() string
	OperandValue(context.Context) (interface{}, error)
}

var _ OperandValuer = (*EmptyInput)(nil)

type EmptyInput struct {
}

func (EmptyInput) Type() string {
	return "Empty"
}

func (EmptyInput) OperandValue(context.Context) (interface{}, error) {
	return 0, nil
}

type Immediate struct {
	Value interface{}
}

func (Immediate) Type() string {
	return "Immediate"
}

func (v Immediate) OperandValue(context.Context) (interface{}, error) {
	return v.Value, nil
}

type QueryConverter string

const QueryConverterFlat QueryConverter = "Flat"
const QueryConverterFirstValue QueryConverter = "FirstValue"

type DatabaseInput struct {
	Driver     string
	DSN        string
	Query      string
	Converters []QueryConverter
}

func NewDatabaseInput(driver, dsn, query string, converters []QueryConverter) DatabaseInput {
	return DatabaseInput{driver, dsn, query, converters}
}

func (v DatabaseInput) Type() string {
	return "Database"
}

func (v DatabaseInput) OperandValue(ctx context.Context) (interface{}, error) {
	db, err := databasePool.Get(v.Driver, v.DSN)
	if err != nil {
		return nil, wrapError(err, "Get a database connection")
	}
	return v.doQuery(ctx, db)
}

func (v DatabaseInput) isFlat() bool {
	for _, c := range v.Converters {
		if c == QueryConverterFlat {
			return true
		}
	}
	return false
}

func (v DatabaseInput) isFirstValue() bool {
	for _, c := range v.Converters {
		if c == QueryConverterFirstValue {
			return true
		}
	}
	return false
}

func (v DatabaseInput) doQuery(ctx context.Context, db *gorm.DB) (interface{}, error) {
	if v.isFirstValue() {
		return v.firstValueQuery(ctx, db)
	}
	if v.isFlat() {
		return v.flatQuery(ctx, db)
	}
	return v.tableQuery(ctx, db)
}

func (v DatabaseInput) firstValueQuery(ctx context.Context, db *gorm.DB) (interface{}, error) {
	rows, err := db.WithContext(ctx).Raw(v.Query).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	typeName := columnTypes[0].DatabaseTypeName()

	for rows.Next() {
		var value interface{}
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		return parseSQLValueIntoRealType(value, typeName)
	}
	return EmptyQueryResult, nil
}

func (v DatabaseInput) flatQuery(ctx context.Context, db *gorm.DB) (interface{}, error) {
	rows, err := db.WithContext(ctx).Raw(v.Query).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	typeName := columnTypes[0].DatabaseTypeName()

	list := []interface{}{}
	for rows.Next() {
		var value interface{}
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}

		value, err = parseSQLValueIntoRealType(value, typeName)
		if err != nil {
			return nil, err
		}

		list = append(list, value)
	}
	return list, nil
}

func (v DatabaseInput) tableQuery(ctx context.Context, db *gorm.DB) (interface{}, error) {
	rows, err := db.WithContext(ctx).Raw(v.Query).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	pointers := make([]interface{}, len(cols))

	list := []Map{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}

		err := rows.Scan(pointers...)
		if err != nil {
			return nil, err
		}

		m := make(Map)
		for i, name := range cols {

			value, err := parseSQLValueIntoRealType(values[i], columnTypes[i].DatabaseTypeName())
			if err != nil {
				return nil, err
			}

			m[name] = value
		}
		list = append(list, m)
	}
	return list, nil
}

func parseSQLValueIntoRealType(value interface{}, columnType string) (interface{}, error) {
	t := strings.ToUpper(columnType)

	if strings.Contains(t, "CHAR") || strings.Contains(t, "TEXT") {
		return cast.ToStringE(value)
	} else if strings.Contains(t, "INT") {
		s, err := cast.ToStringE(value)
		if err != nil {
			return nil, err
		}
		return cast.ToIntE(s)
	} else if strings.Contains(t, "FLOAT") || strings.Contains(t, "DECIMAL") {
		s, err := cast.ToStringE(value)
		if err != nil {
			return nil, err
		}
		return cast.ToFloat64E(s)
	} else if strings.Contains(t, "BLOB") {
		return value, nil
	}

	return nil, newErrorf("Column type %s is not supported", columnType)
}

var DatabaseFactory = func(driver, dsn string) (*gorm.DB, error) {
	switch strings.ToLower(driver) {
	case "mysql":
		return gorm.Open(mysql.Open(dsn), &gorm.Config{})
	case "postgres":
		return gorm.Open(postgres.Open(dsn), &gorm.Config{})
	case "clickhouse":
		return gorm.Open(clickhouse.Open(dsn), &gorm.Config{})
	default:
		return nil, newErrorf("Database driver %s not supported", driver)
	}
}

var databasePool = NewDatabasePool()

type DatabasePool struct {
	conns map[dbConnKey]*gorm.DB
	mu    *sync.RWMutex
}

func NewDatabasePool() DatabasePool {
	return DatabasePool{
		conns: make(map[dbConnKey]*gorm.DB),
		mu:    &sync.RWMutex{},
	}
}

func (p *DatabasePool) Get(driver, dsn string) (*gorm.DB, error) {
	key := dbConnKey{driver, dsn}
	p.mu.RLock()
	db, ok := p.conns[key]
	p.mu.RUnlock()

	if ok {
		return db, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	db, ok = p.conns[key]
	if ok {
		return db, nil
	}

	db, err := DatabaseFactory(driver, dsn)
	if err != nil {
		return nil, err
	}
	p.conns[key] = db

	return db, nil
}

type dbConnKey struct {
	driver string
	dsn    string
}
