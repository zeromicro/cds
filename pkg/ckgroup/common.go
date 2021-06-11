package ckgroup

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unsafe"

	"github.com/dchest/siphash"
	"github.com/tal-tech/go-zero/core/logx"
)

const (
	DRIVER      = "clickhouse"
	DbTag       = "db"
	unknowDB    = `unknow_db`
	unknowTable = `unknow_table`
)

var (
	chanClosedErr    = errors.New("chan is closed ! ")
	chanTpyeErr      = errors.New("chan type must be [chan *sturct] or [chan struct] . ")
	queryRowTypeErr  = errors.New("data type must be *struct . ")
	queryRowsTypeErr = errors.New("data type must be *[]*struct{} . ")
	insertTypeErr    = errors.New("data type must be  []*sturct or []struct ")
	ckDnsErr         = errors.New("parse clickhosue connect string fail . ")
)

var (
	parseInsertSQLRe = regexp.MustCompile(`(?m)#{[0-9a-zA-Z_]+}`)
	tokenRe          = regexp.MustCompile("\\s+")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func parseHostAndUser(str string) (string, string, error) {
	parse, err := url.Parse(str)
	if err != nil {
		return "", "", ckDnsErr
	}
	host := strings.Split(parse.Host, ":")[0]
	if host == "" {
		return "", "", ckDnsErr
	}
	user := parse.Query().Get("username")
	return host, user, nil
}

func findFieldValueByTag(value reflect.Value, tag, tagValue string) (reflect.Value, error) {
	t := value.Type()
	index, err := findFieldIndexByTag(t, tag, tagValue)
	if err != nil {
		return reflect.Value{}, err
	}
	return value.Field(index), nil
}

func findFieldIndexByTag(t reflect.Type, tag, tagValue string) (int, error) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tv, ok := field.Tag.Lookup(tag)
		if !ok {
			continue
		}
		if tv == tagValue {
			return i, nil
		}
	}
	return -1, errors.New("field with tag '" + tag + "' not found")
}

func generateInsertSQL(query string) (string, []string) {
	trueSQL := query
	find := parseInsertSQLRe.FindAllString(query, -1)
	tags := make([]string, 0, len(find))
	for _, match := range find {
		trueSQL = strings.Replace(trueSQL, match, "?", 1)

		match = strings.ReplaceAll(match, "#", "")
		match = strings.ReplaceAll(match, "{", "")
		match = strings.ReplaceAll(match, "}", "")
		tags = append(tags, match)
	}
	return trueSQL, tags
}

func containsComment(query string) bool {
	return strings.Contains(query, `--`) || strings.Contains(query, `/*`)
}

func parseInsertSQLTableName(insertSQL string) (db string, table string) {
	tokens := tokenRe.Split(strings.ToLower(insertSQL), -1)
	var intoIdxs []int

	for i, token := range tokens {
		if token == "into" {
			intoIdxs = append(intoIdxs, i)
		}
	}
	for _, intoIdx := range intoIdxs {
		if intoIdx == 0 && intoIdx == len(tokens)-1 {
			continue
		}
		if tokens[intoIdx-1] == "insert" {
			if tokens[intoIdx+1] == "values" || strings.HasPrefix(tokens[intoIdx+1], "(") {
				continue
			}
			splits := strings.Split(tokens[intoIdx+1], ".")
			if len(strings.Split(tokens[intoIdx+1], ".")) == 2 {
				return splits[0], splits[1]
			} else {
				return unknowDB, splits[0]
			}
		} else {
			continue
		}
	}
	return unknowDB, unknowTable
}

// dest 是指针的 interface
func span(dest interface{}, idx []int) rowValue {
	var result []interface{}
	structVal := reflect.ValueOf(dest).Elem()
	for _, fieldIdx := range idx {
		if fieldIdx != -1 {
			if structVal.NumField() < fieldIdx+1 {
				result = append(result, new(interface{}))
			} else {
				result = append(result, structVal.Field(fieldIdx).Addr().Interface())
			}
		} else {
			result = append(result, new(interface{}))
		}
	}
	return result
}

func getDataBatch(hashIdx int, shardNum int, args []rowValue) ([][]rowValue, error) {
	dataBatch := make([][]rowValue, shardNum)
	for _, item := range args {
		rowValue := item
		hashKey := item[hashIdx]
		idx := siphash.Hash(0, 0, []byte(fmt.Sprint(hashKey))) % uint64(shardNum)
		dataBatch[idx] = append(dataBatch[idx], rowValue)
	}
	return dataBatch, nil
}

func saveData(db *sql.DB, insertSql string, values []rowValue) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(insertSql)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			logx.Error("tx rollback error:", err.Error())
		}
		return err
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			logx.Error("stmt close error:", err.Error())
		}
	}()
	for _, value := range values {
		if _, err := stmt.Exec(value...); err != nil {
			if err := tx.Rollback(); err != nil {
				logx.Error("tx rollback error:", err.Error())
			}
			return err
		}
	}

	return tx.Commit()
}

// val 是 struct 类型的 value
func generateRowValue(val reflect.Value, tags []string) (rowValue, error) {
	args := make(rowValue, 0, len(tags))
	for _, tagVal := range tags {
		fieldVal, err := findFieldValueByTag(val, DbTag, tagVal)
		if err != nil {
			return args, err
		}
		args = append(args, fieldVal.Interface())
	}
	return args, nil
}

// ch 一定要是 chan 类型 , 否则会 painc
func isChanClosed(ch interface{}) bool {
	cptr := *(*uintptr)(unsafe.Pointer(
		uintptr(unsafe.Pointer(&ch)) + unsafe.Sizeof(uint(0)),
	))

	cptr += unsafe.Sizeof(uint(0)) * 2
	cptr += unsafe.Sizeof(uintptr(0))
	cptr += unsafe.Sizeof(uint16(0))
	return *(*uint32)(unsafe.Pointer(cptr)) > 0
}
