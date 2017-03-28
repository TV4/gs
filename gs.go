package gs

import (
	"context"
	"regexp"
	"sort"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

var (
	timeout    = time.Second * 10
	dateLayout = "20060102"
)

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
