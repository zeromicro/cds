package ckgroup

import (
	"reflect"
	"testing"
	"time"
)

func Test_panicIfErr(t *testing.T) {
	panicIfErr(nil)
}

func Test_parseHostAndUser(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   string
		wantErr bool
	}{
		{args: args{`tcp://localhost:9000`}, want: `localhost`, want1: ``, wantErr: false},
		{args: args{`tcp://1.2.2.3:9000?username=default&password=1234&database=xx`}, want: `1.2.2.3`, want1: `default`, wantErr: false},
		{args: args{`tcp://`}, want: ``, want1: ``, wantErr: true},
		{args: args{``}, want: ``, want1: ``, wantErr: true},
	}
	for _, tt := range tests {
		v := tt
		t.Run(v.name, func(t *testing.T) {
			got, got1, err := parseHostAndUser(v.args.str)
			if (err != nil) != v.wantErr {
				t.Errorf("parseHostAndUser() error = %v, wantErr %v", err, v.wantErr)
				return
			}
			if got != v.want {
				t.Errorf("parseHostAndUser() got = %v, want %v", got, v.want)
			}
			if got1 != v.want1 {
				t.Errorf("parseHostAndUser() got1 = %v, want %v", got1, v.want1)
			}
		})
	}
}

func Test_fieldByTag(t *testing.T) {
	testStruct := struct {
		F1 string `db:"f1"`
		F2 int    `json:"f2"`
		F3 string `xx:"f3"`
	}{}
	testVal := reflect.ValueOf(testStruct)
	f1Val := testVal.FieldByName(`F1`)
	f2Val := testVal.FieldByName(`F2`)
	f3Val := testVal.FieldByName(`F3`)
	type args struct {
		value    reflect.Value
		tag      string
		tagValue string
	}
	tests := []struct {
		name    string
		args    args
		want    reflect.Value
		wantErr bool
	}{
		{args: args{testVal, `db`, `f1`}, want: f1Val, wantErr: false},
		{args: args{testVal, `json`, `f2`}, want: f2Val, wantErr: false},
		{args: args{testVal, `xx`, `f3`}, want: f3Val, wantErr: false},
		{args: args{testVal, `aa`, `f3`}, want: reflect.Value{}, wantErr: true},
	}
	for _, tt := range tests {
		v := tt
		t.Run(v.name, func(t *testing.T) {
			got, err := findFieldValueByTag(v.args.value, v.args.tag, v.args.tagValue)
			if (err != nil) != v.wantErr {
				t.Errorf("findFieldValueByTag() error = %v, wantErr %v", err, v.wantErr)
				return
			}
			if !reflect.DeepEqual(got, v.want) {
				t.Errorf("findFieldValueByTag() got = %v, want %v", got, v.want)
			}
		})
	}
}

func Test_parseInsertSQL(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 []string
	}{
		{args: args{`insert into user (id,real_name,city) values (#{id},#{real_name},#{city})`},
			want: `insert into user (id,real_name,city) values (?,?,?)`, want1: []string{`id`, `real_name`, `city`}},
		{args: args{`insert into user (id,real_name,city) values (#{id},#{real-name},#{city})`},
			want: `insert into user (id,real_name,city) values (?,#{real-name},?)`, want1: []string{`id`, `city`}},
		{args: args{`insert into user (id,real_name,city) values (#{real-name})`},
			want: `insert into user (id,real_name,city) values (#{real-name})`, want1: []string{}},
	}
	for _, tt := range tests {
		v := tt
		t.Run(v.name, func(t *testing.T) {
			got, got1 := generateInsertSQL(v.args.query)
			if got != v.want {
				t.Errorf("generateInsertSQL() got = %v, want %v", got, v.want)
			}
			if !reflect.DeepEqual(got1, v.want1) {
				t.Errorf("generateInsertSQL() got1 = %v, want %v", got1, v.want1)
			}
		})
	}
}

func Test_span(t *testing.T) {
	testStruct := struct {
		F1 string
		F2 int
		F3 string
	}{F1: "f1", F2: 1, F3: "f3"}
	structVal := reflect.ValueOf(&testStruct).Elem()
	f1 := structVal.FieldByName("F1").Addr().Interface()
	f2 := structVal.FieldByName("F2").Addr().Interface()
	f3 := structVal.FieldByName("F3").Addr().Interface()

	type args struct {
		dest interface{}
		idx  []int
	}
	tests := []struct {
		name string
		args args
		want []interface{}
	}{
		{args: args{&testStruct, []int{0, 1, 2}}, want: rowValue{f1, f2, f3}},
		{args: args{&testStruct, []int{}}, want: nil},
		{args: args{&testStruct, []int{1, 1, 2}}, want: rowValue{f2, f2, f3}},
	}
	for _, tt := range tests {
		v := tt
		t.Run(v.name, func(t *testing.T) {
			if got := span(v.args.dest, v.args.idx); !reflect.DeepEqual(got, v.want) {
				t.Errorf("span() = %v, want %v", got, v.want)
			}
		})
	}
}

func Test_generateRowValue(t *testing.T) {
	testStruct := struct {
		F1 string `db:"f1"`
		F2 int    `db:"f2"`
		F3 string `db:"f3"`
	}{}
	testVal := reflect.ValueOf(testStruct)
	f1 := testVal.FieldByName(`F1`).Interface()
	f2 := testVal.FieldByName(`F2`).Interface()
	f3 := testVal.FieldByName(`F3`).Interface()

	type args struct {
		val  reflect.Value
		tags []string
	}
	tests := []struct {
		name    string
		args    args
		want    rowValue
		wantErr bool
	}{
		{args: args{testVal, []string{`f1`, `f2`, `f3`}}, want: rowValue{f1, f2, f3}, wantErr: false},
		{args: args{testVal, []string{`f3`, `f2`}}, want: rowValue{f3, f2}, wantErr: false},
		{args: args{testVal, []string{`f4`, `f2`}}, want: rowValue{}, wantErr: true},
	}
	for _, tt := range tests {
		v := tt
		t.Run(v.name, func(t *testing.T) {
			got, err := generateRowValue(v.args.val, v.args.tags)
			if (err != nil) != v.wantErr {
				t.Errorf("generateRowValue() error = %v, wantErr %v", err, v.wantErr)
				return
			}
			if !reflect.DeepEqual(got, v.want) {
				t.Errorf("generateRowValue() got = %v, want %v", got, v.want)
			}
		})
	}
}

func Test_isChanClosed(t *testing.T) {
	type args struct {
		ch interface{}
	}
	ch1 := make(chan int)
	ch2 := make(chan int)
	close(ch2)
	tests := []struct {
		name string
		args args
		want bool
	}{
		{args: args{ch1}, want: false},
		{args: args{ch2}, want: true},
	}
	for _, tt := range tests {
		v := tt
		t.Run(v.name, func(t *testing.T) {
			if got := isChanClosed(v.args.ch); got != v.want {
				t.Errorf("isChanClosed() = %v, want %v", got, v.want)
			}
		})
	}
}

type benchmarkStruct struct {
	A string    `json:"a"`
	B int64     `json:"b"`
	C bool      `json:"c"`
	D time.Time `json:"d"`
	E string    `json:"e"`
}

func Benchmark_findFieldValueByTag(b *testing.B) {
	obj := benchmarkStruct{}
	val := reflect.ValueOf(obj)
	for i := 0; i < b.N; i++ {
		_, _ = findFieldValueByTag(val, "json", "d")
		_, _ = findFieldValueByTag(val, "json", "e")
	}
}

func Test_parseInsertSQLTableName(t *testing.T) {
	type args struct {
		insertSQL string
	}
	tests := []struct {
		name      string
		args      args
		wantDb    string
		wantTable string
	}{
		{args: args{`insert into a.b (a,b,c) values (a,b,c)`}, wantDb: `a`, wantTable: `b`},
		{args: args{`insert into b (a,b,c) values (a,b,c)`}, wantDb: unknowDB, wantTable: `b`},
		{args: args{`insert into  (a,b,c) values (a,b,c)`}, wantDb: unknowDB, wantTable: unknowTable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDb, gotTable := parseInsertSQLTableName(tt.args.insertSQL)
			if gotDb != tt.wantDb {
				t.Errorf("parseInsertSQLTableName() gotDb = %v, want %v", gotDb, tt.wantDb)
			}
			if gotTable != tt.wantTable {
				t.Errorf("parseInsertSQLTableName() gotTable = %v, want %v", gotTable, tt.wantTable)
			}
		})
	}
}

func Test_containsComment(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{args: args{`insert into /* 注释*/
tb (a,b) values (1,2)`}, want: true},
		{args: args{`insert into -- aaa
tb (a,b) values (1,2)`}, want: true},
		{args: args{`insert into tb (a,b) values (1,2)`}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := containsComment(tt.args.query); got != tt.want {
				t.Errorf("containsComment() = %v, want %v", got, tt.want)
			}
		})
	}
}
