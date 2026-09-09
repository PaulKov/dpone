# Dev install mode command examples

## Scheduler

```bash
dpone-runtime-exec airflow scheduler
```

## Worker

```bash
dpone-runtime-exec airflow celery worker
```

## Webserver

```bash
dpone-runtime-exec airflow webserver
```

## Dry-run plan

```bash
dpone-runtime-exec --dry-run -- airflow scheduler
```
