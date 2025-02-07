# foosleeve
Resource Definition:
```
apiVersion: example.com/v1
kind: Foo
metadata:
  name: example
spec:
  mode: A  # Can be "A" or "B"
status:
  state: ""  # Controllers will populate this
```
Possible state transitions:
```
       (init with mode: A)
         |
         v
      [A-1] <---> [B-1]  (one-time switch allowed)
         |              |
         v              v
      [A-2]          [B-2]
         |              |
         v              v
    [A-Final]      [B-Final]
```

Possible Paths:
Each execution follows one of the following finite paths:
1.	No mode switch (direct paths):

`init (mode: A) → A-1 → A-2 → A-Final (1 path)`

`init (mode: A) → B-1 → B-2 → B-Final (1 path)`

2.	With mode switch (detour paths):

`init (mode: A) → A-1 → (flip to mode B) → B-1 → B-2 → B-Final (1 path)`

`init (mode: A) → B-1 → (flip to mode A) → A-1 → A-2 → A-Final (1 path)`
