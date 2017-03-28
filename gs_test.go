package gs

import (
	"context"
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
)

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
	ctx, cancelf := context.WithTimeout(context.Background(), timeout)
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
