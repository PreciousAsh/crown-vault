# Crown Vault

Co-op webcam heist game. Players freeze while a thermal scanner sweeps, then
"torch" between sweeps to fill a shared Crown bar and open the vault.

## Run locally

    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
    .venv/bin/python server.py

Open http://localhost:8642. To try co-op on one machine, open a second tab
with the room link shown in the lobby.

## Deploy to Render (free)

1. Create a new repository on github.com and upload `server.py`,
   `requirements.txt`, `render.yaml`, `.gitignore` and the `static` folder.
   Do not upload `.venv`.
2. On render.com, sign in with GitHub, choose **New > Blueprint**, and pick
   the repository. Render reads `render.yaml` and creates the service.
3. When the deploy finishes, open the `https://<name>.onrender.com` address,
   enter your name, and copy the room link from the lobby for your crew.

Free services sleep after about 15 minutes without visitors; the first visit
after that takes up to a minute to load.

## Exclusive clip

Save the video as `static/clip.mp4`. Without it the vault opens to a
"No clip found" message.

## Tuning

Top of the script in `static/index.html`: raise `MIN_FLAG` or `DIFF` to make
the scanner more forgiving. Scoring lives at the top of `server.py`.
