// package gs contains convenience functions for dealing with Google Storage.
//
// It wraps the cloud.google.com/go/storage package and requires adequate
// credentials to be installed.
package gs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cemkalti/backoff"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

var (
	timeout    = time.Second * 10
	dateLayout = "20060102"
)

// WriteAndCompose writes 'data' to an object identified by 'url'. It does
// this by first creating and writing to a temporary object, and then composing
// the temporary object with the target object, creating the target object
// if it does not exist. If the target object is being updated by another
// process, the function will retry under exponential backoff, for no more
// than 15 minutes.
//
// This can be handy when multiple processes are writing to the same file.
func WriteAndCompose(data []byte, url string) error {
	ctx, cancelf := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelf()

	bkt, pf, name, err := BucketPrefixObject(url)
	if err != nil {
		return err
	}
	path := filepath.Join(pf, name)

	c, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	// Write to temp file
	tmpPath, err := writeToTemp(c, bkt, path, data)
	if err != nil {
		return err
	}

	// Does the object yet exist?
	obj := c.Bucket(bkt).Object(path)
	if _, err := obj.Attrs(ctx); err != nil {
		if err == storage.ErrObjectNotExist {
			if err := obj.NewWriter(ctx).Close(); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	tmpObj := c.Bucket(bkt).Object(tmpPath)
	op := func() error {
		return compose(obj, tmpObj)
	}

	if err = backoff.Retry(op, backoff.NewExponentialBackOff()); err != nil {
		return err
	}

	return nil
}

func writeToTemp(c *storage.Client, bkt, path string, data []byte) (string, error) {
	ctx, cancelf := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelf()

	// Write to temp file
	tempPath := fmt.Sprintf("%s.%d", path, time.Now().Nanosecond())
	obj := c.Bucket(bkt).Object(tempPath)

	w := obj.NewWriter(ctx)
	n, err := w.Write(data)
	if n < len(data) {
		return "", errors.New("failed writing all data")
	}
	if err != nil {
		return "", err
	}

	if err := w.Close(); err != nil {
		return "", err
	}

	return tempPath, nil
}

func compose(obj, pobj *storage.ObjectHandle) error {
	ctx, cancelf := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelf()

	attr, err := obj.Attrs(ctx)
	if err != nil {
		return backoff.Permanent(err)
	}

	pattr, err := pobj.Attrs(ctx)
	if err != nil {
		return backoff.Permanent(err)
	}

	cond := storage.Conditions{GenerationMatch: attr.Generation}
	composer := obj.If(cond).ComposerFrom(pobj, obj)
	composer.ContentType = pattr.ContentType
	if attr, err = composer.Run(ctx); err != nil {
		return err
	}

	if err = pobj.Delete(ctx); err != nil {
		return backoff.Permanent(err)
	}

	return nil
}

// ObjectReader returns a pointer to a storage.Reader for the object identified
// by url (on the form `gs://path-to-object`).
func ObjectReader(url string) (*storage.Reader, error) {
	ctx, cancelf := context.WithTimeout(context.Background(), timeout)
	defer cancelf()

	c, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bkt, pf, name, err := BucketPrefixObject(url)
	if err != nil {
		return nil, err
	}

	return c.Bucket(bkt).Object(filepath.Join(pf, name)).NewReader(ctx)
}

// Has Object returns true if the object identified by url exists and false
// if it does not.
func HasObject(url string) (bool, error) {
	ctx, cancelf := context.WithTimeout(context.Background(), timeout)
	defer cancelf()

	c, err := storage.NewClient(ctx)
	if err != nil {
		return false, err
	}

	bkt, pf, name, err := BucketPrefixObject(url)
	if err != nil {
		return false, err
	}

	_, err = c.Bucket(bkt).Object(filepath.Join(pf, name)).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// BucketPrefixObject decomposes url into its bucket, prefix and name
// components.
func BucketPrefixObject(url string) (string, string, string, error) {
	path := strings.TrimLeft(url, "gs://")
	c := strings.Split(path, "/")
	if len(c) < 2 {
		return "", "", "", errors.New("path does not have bucket and object")
	}

	bkt := c[0]
	prefix := ""
	obj := c[len(c)-1]
	if len(c) > 2 {
		prefix = filepath.Join(c[1 : len(c)-1]...)
	}

	return bkt, prefix, obj, nil
}

// FilesSince returns all objects in a bucket with a given prefix and
// matching a given date pattern, which corresponding date is matching
// or after the given dt. The objects are returned in date order,
// according to their file names.
func ObjectsSince(bkt, prefix, pattern string, dt time.Time) ([]string, error) {
	cmp := func(d, dt time.Time) bool {
		return d.After(dt) || d.Equal(dt)
	}

	return filterObjects(bkt, prefix, pattern, dt, cmp)
}

// FilesBefore returns all objects in a bucket with a given prefix and
// matching a given date pattern, which corresponding date is before
// (not including) the given dt. The objects are returned in date order,
// according to their file names.
func ObjectsBefore(bkt, prefix, pattern string, dt time.Time) ([]string, error) {
	cmp := func(d, dt time.Time) bool {
		return d.Before(dt)
	}

	return filterObjects(bkt, prefix, pattern, dt, cmp)
}

func filterObjects(bkt, prefix, pattern string, dt time.Time, cmp func(d1, d2 time.Time) bool) ([]string, error) {
	ctx, cancelf := context.WithTimeout(context.Background(), timeout)
	defer cancelf()

	matcher, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	dt = time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, dt.Location())

	c, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	q := &storage.Query{Prefix: prefix}
	iter := c.Bucket(bkt).Objects(ctx, q)
	var objs []string
	for {
		o, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		m := matcher.FindStringSubmatch(o.Name)
		if len(m) != 2 {
			continue
		}

		d, err := time.Parse(dateLayout, m[1])
		if err != nil {
			return nil, err
		}

		if cmp(d, dt) {
			objs = append(objs, o.Name)
		}
	}

	sort.Strings(objs)

	return objs, nil
}
