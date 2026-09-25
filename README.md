# MoST Local API

This folder contains a local Node.js API to expose experiment data under `../results` for a local dashboard.

## Run

```bash
cd api
npm install
npm start
```

By default, the API listens on `http://localhost:4000`.

## Endpoints

- `GET /health`
- `GET /api/llm-name`
- `GET /api/gpu-used` — GPU label plus the model endpoint `url` read from the project `.env` (`URL`, `FMPERF_ENDPOINT_URL` or `ENDPOINT_URL`), which the dashboard uses to resolve the node and port for `job-gpu-count`.
- `GET /api/results-scopes`
- `GET /api/experiments` — returns `experiments`, the plain list of experiment folder names, plus `additiveExperiments`, the additive (`WORKLOAD_MIXES`) subset decoded from the `mix_...` folder names (`name`, `isAdditive`, `canonical`, `profiles`, `envelope`).
- `GET /api/experiment-status` — checks whether the experiment is currently running by comparing the most recent `slurm-XXXXX.out` job id against the ids reported by `squeue`.
- `GET /api/experiment-log?lines=N` — returns the content of the most recent `slurm-XXXXX.out` log. Without `lines`, the whole file is returned; with `lines=N`, only the last `N` lines are returned.
- `GET /api/job-gpu-count?model=MODEL_ID&node=NODE&port=PORT` — returns the number of GPUs used by the Slurm job serving the given model on the given node and port. Delegates to the shared `MoST-experiment-environment/fmperf/utils/GpuCount.py` helper (`squeue` + `scontrol show job`, `TresPerJob`). When the job cannot be inspected (e.g. it already finished), it falls back to the `GPU_COUNT` stored in the latest `results.csv` of the requested results scope.
- `GET /api/experiments/:experiment/iterations`
- `GET /api/experiments/:experiment/iterations/:iteration/results.csv` — parsed rows plus an `additive` descriptor (see Notes).
- `GET /api/experiments/:experiment/iterations/:iteration/download/results.csv`
- `GET /api/experiments/:experiment/iterations/:iteration/download/results.json`

## Notes

- The API reads `../.env` first (project root), then `api/.env` if present.
- `llm-name` and `gpu-used` are resolved from environment values first, then latest iteration result files.
- The `results.csv` endpoint returns parsed rows as JSON for dashboard consumption.
- The parsed `results.csv` endpoint accepts an optional `fields` query parameter, for example `?fields=MODEL_USED,URL,LARGEST_TRUE`, to return only selected CSV columns. Without `fields`, it returns all columns.
- Download endpoints return the raw files.
- Experiment endpoints accept `?resultsScope=current|MST_1|MST-2|...`.
- `current` reads from `results/` directly, while other scopes read from subfolders like `results/MST_1/`.
- Slurm log files named `slurm-XXXXX.out` are read directly from `MOST_PROJECT_ROOT` (the project root). The most recent log is the one with the largest numeric job id. When `squeue` is unavailable, `experiment-status` reports `isRunning: null` and `squeueAvailable: false` instead of failing.
- `job-gpu-count` delegates to the shared script `<MOST_PROJECT_ROOT>/MoST-experiment-environment/fmperf/utils/GpuCount.py` (spawned as `python GpuCount.py find --model ... --node ... --port ...`), which lists the running jobs on the requested node with `squeue`, inspects each with `scontrol show job <id>`, and reads the GPU count from `TresPerJob`. When `squeue`/`scontrol` are unavailable it responds `503 SQUEUE_UNAVAILABLE`; when no job matches the model, node and port it responds `404 JOB_NOT_FOUND`; if the job cannot be inspected, the endpoint falls back to the `GPU_COUNT` column of the latest iteration `results.csv` and returns it with `source: "results/..."`.
- Additive experiments (`WORKLOAD_MIXES`) are not tied to a token-interval matrix cell. Their experiment folder is named after the canonical mix (`mix_<in-min>-<in-max>_<out-min>-<out-max>@<alpha>+...`), the four `MIN/MAX_INPUT/OUTPUT_TOKENS` columns are intentionally blank in `results.csv`, and the run is described by the `WORKLOAD_MIX` (canonical mix, e.g. `[(1-100:1-100,0.5),(300-600:100-300,0.5)]`) and `ADDITIVE` / `ADDITIVE_EXPECTED_PROPORTIONS` / `ADDITIVE_TRUE_PROPORTIONS` (JSON keyed by profile label, e.g. `{"1-100:1-100":0.5,"300-600:100-300":0.5}`) columns instead. Additive runs are archived as `results/<scope>/Experiment_MIX_<EXPERIMENT_TYPE>_<timestamp>/`, which the existing scope detection already picks up.
- Place an additive experiment with the `additive` descriptor of the `results.csv` endpoint instead of the blank token-interval columns: `isAdditive`, `canonical`, `profiles` (each with `label`, `inMin`, `inMax`, `outMin`, `outMax`, `alpha`), `envelope` (`inMin`/`inMax`/`outMin`/`outMax` of the whole mix), `expectedProportions` and `trueProportions`. For a non-additive experiment it is simply `{"isAdditive": false}`. The descriptor is derived from the full `results.csv` rows before the `fields` filter is applied (so it survives `?fields=`), falls back to the `WORKLOAD_MIX` column and then to the experiment folder name, and matches the environment's `workload_mix.py` rendering (normalised alphas, compact formatting).
