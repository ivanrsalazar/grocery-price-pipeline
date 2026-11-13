# Four-city grocery basket dashboard

This directory contains a self-contained static copy of the portfolio
dashboard. It does not need a build step or a Node.js installation.

## Run in the background

From the repository root, replace `8085` with any available port.

Start the dashboard in the background:

```bash
./sites/four-city-grocery-basket/dashboard.sh start 8085
```

Check whether it is still running:

```bash
./sites/four-city-grocery-basket/dashboard.sh status 8085
```

Stop the process:

```bash
./sites/four-city-grocery-basket/dashboard.sh stop 8085
```

Open <http://127.0.0.1:8085> while it is running. Logs and the PID file are
stored in the ignored `.runtime/` directory. The default port is `8085` when no
port is provided.

## Other serving options

To run the server in the foreground instead, use:

```bash
./sites/four-city-grocery-basket/serve.sh 8085
```

The server binds to `127.0.0.1` so it is only reachable from this Mac. To make
the background server reachable from other devices on the same network,
explicitly provide a bind address:

```bash
./sites/four-city-grocery-basket/dashboard.sh start 8085 0.0.0.0
```

All files served to the browser live in `dist/`, so the directory can also be
copied to any static web server.
