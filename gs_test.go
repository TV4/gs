package gs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/storage"
)

var (
	bkt    = "bb-dev"
	prefix = "gs-test"
	objs   = []string{
		"testobj_20170103.txt",
		"testobj_20170102.txt",
		"testobj_20170101.txt",
		"testobj_20170104.txt",
	}
	text = `This is a test string`
)

func TestAppend(t *testing.T) {
	name := "myfile.txt"
	url := "gs://%s/%s"
	url = fmt.Sprintf(url, bkt, filepath.Join(prefix, name))

	a := Appender{Gzip: true}

	ctx, cancelf := context.WithTimeout(context.Background(), opTimeout)
	defer cancelf()

	err := a.Append(ctx, []byte(text), url)
	if err != nil {
		t.Error(err)
	}

	c, err := storage.NewClient(ctx)
	if err != nil {
		t.Error(err)
	}

	o := filepath.Join(prefix, name+".gz")
	obj := c.Bucket(bkt).Object(o)
	attr, err := obj.Attrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if attr == nil {
		t.Fatal("object not found")
	}
	if attr.Name != o {
		t.Fatal("unknown object found: ", attr.Name)
	}

	_, err = obj.NewReader(ctx)
	if err != nil {
		t.Fatal(err)
	}

	obj.Delete(ctx)

}

func TestBucketPrefixObject(t *testing.T) {
	for _, tst := range []struct {
		url  string
		bkt  string
		pf   string
		name string
	}{
		{"gs://bkt/name", "bkt", "", "name"},
		{"gs://bkt/pf/name", "bkt", "pf", "name"},
		{"gs://bkt/pf1/pf2/name", "bkt", "pf1/pf2", "name"},
	} {
		bkt, pf, name, _ := BucketPrefixObject(tst.url)
		if bkt != tst.bkt {
			t.Errorf("expected %s, got %s", tst.bkt, bkt)
		}
		if pf != tst.pf {
			t.Errorf("expected %s, got %s", tst.pf, pf)
		}
		if name != tst.name {
			t.Errorf("expected %s, got %s", tst.name, name)
		}
	}
}

func TestObjectsBefore(t *testing.T) {
	dt, _ := time.Parse("20060102", "20170103")
	objs, err := ObjectsBefore(bkt, prefix, `testobj_(\d{8}).txt`, dt)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if len(objs) != 2 {
		t.Error("unexpected number of objects:", len(objs))
	}

	if objs[0] != "testobj_20170101.txt" ||
		objs[1] != "testobj_20170102.txt" {
	}
}

func TestObjectsSince(t *testing.T) {
	dt, _ := time.Parse("20060102", "20170102")
	objs, err := ObjectsSince(bkt, prefix, `testobj_(\d{8}).txt`, dt)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if len(objs) != 3 {
		t.Error("unexpected number of objects:", len(objs))
	}
}

func TestMain(m *testing.M) {
	ctx, cancelf := context.WithTimeout(context.Background(), opTimeout)
	defer cancelf()

	c, err := storage.NewClient(ctx)
	if err != nil {
		os.Exit(1)
	}

	for _, o := range objs {
		o = filepath.Join(prefix, o)
		c.Bucket(bkt).Object(o).NewWriter(ctx).Close()
	}

	ret := m.Run()

	for _, o := range objs {
		o = filepath.Join(prefix, o)
		c.Bucket(bkt).Object(o).Delete(ctx)
	}

	os.Exit(ret)
}
