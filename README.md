# gs

`gs` provides convenience functions for working with Google Storage.

### Usage

Assuming there are date objects with names like `dateobject_20060102.txt`

```
# Get the names of the latest three objects
dt := time.Now().AddDate(0,0,-3)
objs, err := gs.ObjectsSince(bkt, prefix, `dateobject_(\d{8}).txt`, dt)
if err != nil {
	log.Fatal(err)
}

```
