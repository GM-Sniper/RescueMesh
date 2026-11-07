# RescueMesh demo panel

A web control panel for the Phase 1 demo on the 6 lab PCs. It runs on one PC that can SSH to all 6,
starts and stops everything, and shows the whole system live. It is demo tooling only: it runs the same
commands as the runbook and reads the logs the programs already write.

## Before the demo (once)

1. On every lab PC: the project is built (`cargo build --release`) in the same folder,
   e.g. `~/Group3/RescueMesh`, and the node PCs have the `weed` binary and 100 resources.
2. On the control PC: Python 3.8+ and `ssh`. Log in without a password to each lab PC:
   ```bash
   ssh-keygen                     # once, press Enter for every question
   ssh-copy-id user@<each lab PC IP>
   ```
3. Start the panel and open it in a browser:
   ```bash
   python3 demo/server.py         # then open http://localhost:8080
   ```

## Using it

- **Setup:** fill in the 6 IPs (and SSH user if it differs), save, then press **One-click bring-up**.
  It checks the PCs, writes `config/demo.toml` on all 6, starts SeaweedFS on N1-N3 and the 3 nodes.
  The Live link column turns green when the panel sees each PC.
- **Mission Control:** start load, mute a node, put CPU stress on a node, run the duplicate test and the
  state check. Everything on screen comes from the live logs.
- **Demo Script:** the 8 acts of the demo with what to say, the buttons, and checks that tick themselves.
- **Consoles:** the output of every command the panel ran, per PC, and each PC's live log.
- **Lab Results:** the measured numbers from the Nov 4-5 lab runs.

Each **Start load** uses fresh seeds, so no wipe is needed between demo runs.
If the page shows "panel server unreachable" behind a proxy, open `http://localhost:8080/?poll`.

## Files

| file | what it does |
|---|---|
| `server.py` | settings, SSH commands, joins the live data, serves the page |
| `agent.py` | sent over SSH to each lab PC; streams new log rows and CPU/memory once a second |
| `static/` | the page (`index.html`, `style.css`, `app.js`) |
| `settings.json` | your saved settings (created on first save, not committed) |
| `collected/` | logs downloaded with **Collect logs** |
