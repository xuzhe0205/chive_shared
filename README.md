# chive-shared

Shared storage models for CHIVE V2.

Consumed by:
- chive (the pipeline + daemon)
- chive-backend (the FastAPI cloud webserver)

## Local development

Both downstream repos use `[tool.uv.sources]` editable overrides pointing here:
```toml
[tool.uv.sources]
chive-shared = { path = "../chive_shared", editable = true }
```

Cloud deploys resolve from `git+https://github.com/xuzhe0205/chive_shared@v0.1.0`.
