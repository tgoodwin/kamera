# Trace Store

Infrastructure for receiving logs exported from instrumented controllers

setting up port forwarding:
need to expose both the API port (9000) as well as the console port (9001)
```
kubectl port-forward svc/minio-svc -n sleeve-system 9000:9000 9001:9001
```

Then, you can set an alias
```mc alias set local http://localhost:9000 sleeveadmin sleevepassword
```
then you can call that alias
```
mc ls local
```

once that works, lets create a user. we'll call the user "sleeve" and add it to the "local" minio service we created above.

```
mc admin user add local sleeve sleevesecretkey
```

next, let's assign a policy to this user
```
mc admin policy attach local readwrite --user=sleeve
```

after that, lets create a service account (under the "sleeve" user) that controllers will use to write data.

```
mc admin user svcacct add local sleeve --access-key "myaccesskey" --secret-key "mysecretkey"
```

n.b. this accesskey/secretkey pair is currently hardcoded into the minio_emitter.go code. TODO externalize this config somewhere.

## collecting trace data
use a parallelized script to read data back down.
```
go run cmd/collect/main.go --bucket <BUCKET_NAME>
