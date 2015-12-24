# GrnCI

## Groonga Command Interface (Test version)

Groonga コマンドに対する Go インタフェースのテストと評価を目的としています．

### Create()

DB を作成してハンドルを作成します．

### Open()

DB を開いてハンドルを作成します．

### Dup()

DB ハンドルを増やします．

### Close()

DB ハンドルを閉じます．

### DB.Load()

`load` コマンドを実行して，更新されたレコードの数を返します．

```go
func (db *DB) Load(tbl string, vals interface{}, options *LoadOptions) (int, error)
```

- 7.3.20. load — Groonga v5.1.0ドキュメント
 - http://groonga.org/ja/docs/reference/commands/load.html

`vals` にはレコードに対応する構造体，そのポインタおよびスライスを渡すことができます．
構造体の `groonga` タグを付与されたフィールドのみが `load` に渡されます．

以下，構造体と使い方の例です．

```go
type Value struct {
	Key  grnci.Text  `groonga:"_key"`
	ColA grnci.Bool  `groonga:"ColA"`
	ColB grnci.Text  `groonga:"ColB"`
	ColC []grnci.Int `groonga:"ColC"`
}
```

```go
var val Value
val.Key = "orange"
val.ColA = "false"
val.ColB = "delicious"
val.ColC = []grnci.Int{100, 200, 300}
if err := db.Load("Fruit", val); err != nil {
	log.Fatal(err)
}
```

以下のような制限事項があります．

- 構造体の特定フィールドのみを更新することができません．
 - `map[key]value` のサポートで対応できます．
  - 内部で JSON の配列表現が使えなくなります．
 - ポインタおよびスライスの `nil` を更新しないフィールドとして使うという選択肢があります．
  - 内部で JSON の配列表現が使えなくなります．
 - `LoadOptions` に `--columns` を追加することで対応できます．
  - `--columns` が指定されたときは，指定されたフィールドのみを更新します．
  - レコードにより異なるフィールドを更新するようなことはできません．
- 専用のデータ型を使います．
 - 使えるデータ型は `grnci.Bool`, `grnci.Int`, `grnci.Float`, `grnci.Time`, `grnci.Text`, `grnci.Geo` とこれらのスライスのみです．
