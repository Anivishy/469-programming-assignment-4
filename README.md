# Distributed Search Engine, Part 1

This is the current version of the distributed search engine project for CPE 469.

Right now the system does this:

- one coordinator hands out crawl/map work
- workers crawl URLs in batches of 100
- each map task writes intermediate files split by reduce partition
- mappers push non-local intermediate files to the worker that owns that reduce partition
- reduce workers combine all of their partition files into final output
- after reduce finishes, the coordinator starts a replication phase for final reduce outputs
- reduce outputs get copied onto separate workers and stored as replicas
- workers heartbeat the coordinator so tasks can be reassigned if a worker dies

## Repo layout

- `main.go`: entry point, runs as either coordinator or worker depending on `ROLE`
- `coordinator/coordinator.go`: coordinator RPC server and task scheduling
- `worker/worker.go`: worker RPC server, crawler, map logic, reduce logic
- `common/data.go`: shared RPC structs
- `docker-compose.yml`: easiest way to run the whole cluster
- `seed_urls.txt`: starting URLs for the crawl

## How the crawl works

- The coordinator reads `seed_urls.txt`.
- URLs are batched into map tasks of 100 URLs each.
- The total crawl size is controlled by `MAX_VISITED_URLS`.
- Example: if `MAX_VISITED_URLS=900`, the coordinator will create up to 9 map tasks.
- Newly discovered links go back to the coordinator frontier, but the coordinator will stop creating new map tasks once the visited URL cap is reached.

## How file naming works

Intermediate files look like this:

- `map-000-reduce-001.json`

That means:

- `map-000`: this came from map task `0`, which is the first 100-URL batch
- `reduce-001`: this shard belongs to reduce partition `1`

So yes, `map-000-reduce-001.json` is the intermediate shard for the 1st reduce task from the 0th map batch, using zero-based indexing.

Final reduce outputs look like this:

- `reduce-000.json`
- `reduce-001.json`
- `reduce-002.json`
- `reduce-003.json`

Each reduce worker writes the output for its own partition.

## How to use

From the repo root:

```powershell
docker compose down -v
docker compose up --build
```

That will:

- build the image
- start the coordinator
- start all 4 workers
- stream logs in one place

If all the workers exit with code `0`, that usually just means they finished normally.

## Main config knobs

These are the main env vars being used right now:

- `NUM_REDUCE_TASKS`: how many reduce partitions to make
- `NUM_WORKERS`: how many workers are in the cluster
- `MAX_VISITED_URLS`: total number of URLs to crawl, still batched 100 per map task
- `NUM_OUTPUT_REPLICAS`: how many extra final-output copies to keep besides the main reduce output

The defaults in `docker-compose.yml` are:

- `NUM_REDUCE_TASKS=4`
- `NUM_WORKERS=4`
- `MAX_VISITED_URLS=900`
- `NUM_OUTPUT_REPLICAS=1`

If you want a faster smoke test, set `MAX_VISITED_URLS` to `100` or `200`.

## How to inspect outputs

Worker data is stored under `/app/data` inside each worker container.

The two important folders are:

- `/app/data/intermediate`
- `/app/data/reduce`
- `/app/data/replica`

After the run finishes, you can inspect files like this:

```powershell
docker compose run --rm --entrypoint sh worker1 -lc "find /app/data -type f | sort"
docker compose run --rm --entrypoint sh worker2 -lc "find /app/data -type f | sort"
docker compose run --rm --entrypoint sh worker3 -lc "find /app/data -type f | sort"
docker compose run --rm --entrypoint sh worker4 -lc "find /app/data -type f | sort"
```

Mappers keep the local shard for their own reduce partition if they own it, and push the other shards to the right worker.