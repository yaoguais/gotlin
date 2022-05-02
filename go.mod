module github.com/yaoguais/gotlin

go 1.17

require (
	github.com/gofrs/uuid v4.2.0+incompatible
	github.com/heimdalr/dag v0.0.0-00010101000000-000000000000
	github.com/pkg/errors v0.9.1
	github.com/spf13/cast v1.4.1
	github.com/stretchr/testify v1.7.0
	gorm.io/driver/clickhouse v0.3.2
	gorm.io/driver/mysql v1.3.3
	gorm.io/driver/postgres v1.3.5
	gorm.io/gorm v1.23.5
)

require (
	github.com/ClickHouse/clickhouse-go v1.5.4 // indirect
	github.com/cloudflare/golz4 v0.0.0-20150217214814-ef862a3cdc58 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/hashicorp/go-version v1.4.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.12.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.11.0 // indirect
	github.com/jackc/pgx/v4 v4.16.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)

replace github.com/heimdalr/dag => github.com/yaoguais/dag v1.0.2-0.20220501172848-2250958aa8ea
