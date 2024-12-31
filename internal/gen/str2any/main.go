package main

import (
	"bufio"
	"io"
	"iter"
	"os"
	"slices"
	"text/template"
)

var tmpl *template.Template = template.Must(
	template.ParseFiles("./internal/gen/str2any/str2any.tmpl"),
)

type TypePair struct {
	TypeHint  string
	Primitive string
}

var typePairs iter.Seq[TypePair] = func(
	yield func(TypePair) bool,
) {
	var rawPairs iter.Seq2[string, string] = func(
		y func(string, string) bool,
	) {
		y("String", "string")
		y("Int", "int32")
		y("Long", "int64")
		y("Float", "float32")
		y("Double", "float64")
		y("Boolean", "bool")
		y("Uuid", "gu.UUID")
	}

	for hint, prim := range rawPairs {
		yield(TypePair{
			TypeHint:  hint,
			Primitive: prim,
		})
	}
}

type Data struct {
	Pairs []TypePair
}

var data Data = Data{
	Pairs: slices.Collect(typePairs),
}

var filename string = "type2converter.go"

func TemplateToWriter(t *template.Template, w io.Writer) error {
	var bw *bufio.Writer = bufio.NewWriter(w)
	defer bw.Flush()
	return t.Execute(bw, data)
}

func TemplateToFileLike(t *template.Template, f io.WriteCloser) error {
	defer f.Close()
	return TemplateToWriter(t, f)
}

func TemplateToFilename(t *template.Template, filename string) error {
	f, e := os.Create(filename)
	if nil != e {
		return e
	}
	return TemplateToFileLike(t, f)
}

func main() {
	e := TemplateToFilename(tmpl, filename)
	if nil != e {
		panic(e)
	}
}
