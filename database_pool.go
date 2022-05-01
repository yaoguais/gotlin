package gotlin

import (
	"strings"
	"sync"

	"github.com/pkg/errors"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DatabaseFactory = func(driver, dsn string) (*gorm.DB, error) {
	switch strings.ToLower(driver) {
	case "mysql":
		return gorm.Open(mysql.Open(dsn), &gorm.Config{})
	case "postgres":
		return gorm.Open(postgres.Open(dsn), &gorm.Config{})
	case "clickhouse":
		return gorm.Open(clickhouse.Open(dsn), &gorm.Config{})
	default:
		return nil, errors.Errorf("Database driver %s not supported", driver)
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
