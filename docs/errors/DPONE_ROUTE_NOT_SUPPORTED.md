# DPONE_ROUTE_NOT_SUPPORTED

The requested source, sink, and strategy combination is not declared by the
canonical route catalog.

Inspect supported routes and beginner recipes:

```bash
dpone recipe list --source <source> --sink <sink> --strategy <strategy>
```

Choose a listed route or ask the platform owner to certify and publish the
missing runtime route. dpone does not create partial pipeline files for this
error.
