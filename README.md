# GrnCI

## Groonga Command Interface (Test version)

Groonga コマンドに対する Go インタフェースのテストと評価を目的としています．

### Create()

ローカル DB を作成してハンドルを作成します．

### Open()

ローカル DB を開いてハンドルを作成します．

### Connect()

サーバに接続します．

### DB.Dup()

ローカル DB へのハンドルもしくはサーバへの接続を複製します．

### DB.Close()

ローカル DB へのハンドルもしくはサーバへの接続を閉じます．

### DB.TableCreate()

`table_create` コマンドを実行します．

```go
// TableCreateOptions is a set of options for `table_create`.
//
// http://groonga.org/docs/reference/commands/table_create.html
type TableCreateOptions struct {
	Flags            string
	KeyType          string
	ValueType        string
	DefaultTokenizer string
	Normalizer       string
	TokenFilters     string
}

// NewTableCreateOptions() returns the default options.
func NewTableCreateOptions() *TableCreateOptions

// TableCreate() executes `table_create`.
//
// If options is nil, TableCreate() uses the default options.
//
// If options.Flags does not contain TABLE_NO_KEY and options.KeyType is empty,
// TABLE_NO_KEY is automatically added to options.Flags.
//
// http://groonga.org/docs/reference/commands/table_create.html
func (db *DB) TableCreate(name string, options *TableCreateOptions) error
```

**TBW**

### DB.ColumnCreate()

`column_create` コマンドを実行します．

```go
// ColumnCreateOptions is a set of options for `column_create`.
//
// `column_create` takes --flags as a required argument but ColumnCreateOptions
// has Flags because users can specify COLUMN_* via an argument typ of
// ColumnCreate().
// This also means that COLUMN_* should not be set manually.
//
// `column_create` takes --source as an option but ColumnCreateOptions does not
// have Source because users can specify --source via an argument typ of
// ColumnCreate().
//
// http://groonga.org/docs/reference/commands/column_create.html
type ColumnCreateOptions struct {
	Flags string
}

// NewColumnCreateOptions() returns the default options.
func NewColumnCreateOptions() *ColumnCreateOptions

// ColumnCreate() executes `column_create`.
//
// If typ starts with "[]", COLUMN_VECTOR is added to --flags.
// Else if typ contains ".", COLUMN_INDEX is added to --flags.
// In this case, the former part (before '.') is used as --type and the latter
// part (after '.') is used as --source.
// Otherwise, COLUMN_SCALAR is added to --flags.
//
// If options is nil, ColumnCreate() uses the default options.
//
// http://groonga.org/docs/reference/commands/column_create.html
func (db *DB) ColumnCreate(tbl, name, typ string, options *ColumnCreateOptions) error
```

**TBW**

### DB.Load()

`load` コマンドを実行して，更新されたレコードの数を返します．

```go
func (db *DB) Load(tbl string, vals interface{}, options *LoadOptions) (int, error)
```

- 7.3.20. load — Groonga v5.1.0ドキュメント
 - http://groonga.org/ja/docs/reference/commands/load.html

`vals` にはレコードに対応する構造体，そのポインタおよびスライスを渡すことができます．
データ型が `grnci.Bool`, `grnci.Int` などになっているフィールドのみが `load` に渡されます．
基本的にはフィールド名がカラム名として採用されます．
フィールドに `grnci` タグを付与することで，フィールド名とは異なるカラム名を指定することもできます．

以下，構造体と使い方の例です．

```go
type Value struct {
	Key  grnci.Text  `grnci:"_key"`
	ColA grnci.Bool  `grnci:"ColA"`
	ColB grnci.Text  `grnci:"ColB"`
	ColC []grnci.Int `grnci:"ColC"`
}
```

```go
var val Value
val.Key = "orange"
val.ColA = "false"
val.ColB = "delicious"
val.ColC = []grnci.Int{100, 200, 300}
if err := db.Load("Fruit", val, nil); err != nil {
	log.Fatal(err)
}
```

`LoadOptions` を使えば `--columns` や `--ifexists` を指定できます．
以下の例では `--columns` により更新するカラムを制限しています．

```go
options := grnci.NewOptions()
options.Columns = "_key,ColB"
if err := db.Load("Fruit", val, options); err != nil {
	log.Fatal(err)
}
```

以下のような注意点があります．

- レコードにより異なるフィールドを更新することはできません．
 - 別々に `Load()` を呼び出せば更新できます．
- 専用のデータ型を使います．
 - 使えるデータ型は `grnci.Bool`, `grnci.Int`, `grnci.Float`, `grnci.Time`, `grnci.Text`, `grnci.Geo` とこれらのポインタ，スライス，およびにポインタのスライスのみです．
