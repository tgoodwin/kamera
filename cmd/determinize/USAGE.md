# Determinizing Go code + module dependencies

1. create temp dirs
```
mkdir -p ~/tmp/gomodcache ~/tmp/gocache
```

2. install target deps into those directories
```
cd examples/knative-serving
GOMODCACHE=~/tmp/gomodcache go mod download
```

3. update permissions for the files now in the temp dirs
```
chmod -R u+rwX ~/tmp/gomodcache
```

4. Determinize the dependencies you care about
```
go run ~/projects/kamera/cmd/determinize/main.go ~/tmp/gomodcache/knative.dev/
```

5. run the simulation against newly determinized code!
```
cd examples/knative-serving
GOMODCACHE=~/tmp/gomodcache GOCACHE=~/tmp/gocache go run . --depth 10
```



