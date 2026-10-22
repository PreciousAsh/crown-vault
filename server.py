"""Crown Vault co-op server.

Serves the game page and runs one shared game per room over WebSockets.
The server owns the clock and the shared state (phase timing, Crown bar,
team flags). Each player's browser does its own camera work and reports
flags, torch progress and motion level back here. Small thermal frames
from each player are relayed to the rest of the room.
"""
import asyncio
import os
import random
import re
import struct
import time

from aiohttp import WSMsgType, web

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

STEP = 25          # Crown % each time the whole crew completes a torch
MAX_FLAGS = 3      # team flags before everyone is caught
MAX_PLAYERS = 6
MAX_FRAME = 20000  # largest thermal frame (bytes) relayed between players
TORCHES = ["tap", "flash", "jam"]

rooms = {}


def now_ms():
    return time.time() * 1000


class Room:
    def __init__(self, code):
        self.code = code
        self.players = {}  # websocket -> player dict
        self.task = None
        self.next_id = 1
        self.event_id = 0
        self.reset()

    def reset(self):
        self.phase = "lobby"
        self.start = now_ms()
        self.end = None
        self.progress = 0
        self.strikes = 0
        self.torch = None
        self.last_torch = None
        self.beam_dir = 1
        self.beam_dur = 3.4
        self.event = None
        for p in self.players.values():
            p.update(torch=0, done=False, flagged=False)

    def set_phase(self, phase, seconds=None):
        self.phase = phase
        self.start = now_ms()
        self.end = self.start + seconds * 1000 if seconds else None

    def set_event(self, kind, title, text):
        self.event_id += 1
        self.event = {"id": self.event_id, "kind": kind, "title": title, "text": text}

    def snapshot(self, me):
        return {
            "t": "state",
            "now": now_ms(),
            "room": self.code,
            "you": me,
            "phase": self.phase,
            "start": self.start,
            "end": self.end,
            "torch": self.torch,
            "beamDir": self.beam_dir,
            "beamDur": self.beam_dur,
            "progress": self.progress,
            "strikes": self.strikes,
            "maxFlags": MAX_FLAGS,
            "event": self.event,
            "players": [
                {k: p[k] for k in ("id", "name", "motion", "torch", "done", "flagged")}
                for p in self.players.values()
            ],
        }

    async def broadcast(self):
        for ws, p in list(self.players.items()):
            try:
                await ws.send_json(self.snapshot(p["id"]))
            except Exception:
                pass

    async def wait_phase(self, seconds):
        await self.broadcast()
        await asyncio.sleep(seconds)

    async def scan(self):
        """Sweep the beam until nobody moves. Returns False if the crew got caught."""
        while True:
            for p in self.players.values():
                p["flagged"] = False
            self.event = None
            self.set_phase("warn", 1.2)
            await self.wait_phase(1.2)
            self.set_phase("sweep", self.beam_dur)
            await self.wait_phase(self.beam_dur)
            self.beam_dir *= -1
            if self.strikes >= MAX_FLAGS:
                self.set_phase("caught")
                await self.broadcast()
                return False
            if not any(p["flagged"] for p in self.players.values()):
                return True
            self.set_event("bad", "Rescan", "Someone moved. The whole crew gets scanned again.")
            self.set_phase("result", 2)
            await self.wait_phase(2)

    async def run(self):
        try:
            self.set_phase("calib", 2.5)
            await self.wait_phase(2.5)
            if not await self.scan():
                return
            self.set_event("good", "Scan clear", "Nobody moved. Time to torch the vault.")
            self.set_phase("result", 2)
            await self.wait_phase(2)

            # Torch rounds wait until every player has completed them; a scan follows each one.
            while True:
                self.torch = random.choice([t for t in TORCHES if t != self.last_torch])
                self.last_torch = self.torch
                for p in self.players.values():
                    p.update(torch=0, done=False)
                self.set_phase("torch")
                await self.broadcast()
                while not (self.players and all(p["done"] for p in self.players.values())):
                    await asyncio.sleep(0.05)
                self.progress = min(100, self.progress + STEP)
                if self.progress >= 100:
                    self.set_event("good", "Burn through", "The whole crew torched. The vault is open.")
                    self.set_phase("result", 1.5)
                    await self.wait_phase(1.5)
                    break
                self.set_event("good", "Burn through", "Crown %d%%. Scanner coming back, freeze!" % self.progress)
                self.set_phase("result", 1.8)
                await self.wait_phase(1.8)
                if not await self.scan():
                    return

            self.set_phase("vault")
            await self.broadcast()
        except asyncio.CancelledError:
            pass

    def start_game(self):
        if self.task and not self.task.done():
            return
        self.reset()
        self.task = asyncio.ensure_future(self.run())

    async def on_message(self, player, msg):
        kind = msg.get("t")
        if kind == "start" and self.phase in ("lobby", "caught", "vault"):
            self.start_game()
        elif kind == "lobby" and self.phase in ("caught", "vault"):
            self.reset()
            await self.broadcast()
        elif kind == "flag" and self.phase == "sweep" and not player["flagged"]:
            player["flagged"] = True
            self.strikes += 1
            self.set_event("bad", "Flagged", "%s moved under the beam. Team flag %d of %d" % (player["name"], self.strikes, MAX_FLAGS))
            await self.broadcast()
        elif kind == "torch" and self.phase == "torch" and not player["done"]:
            v = min(1.0, max(0.0, float(msg.get("v", 0))))
            player["torch"] = v
            player["done"] = v >= 1
            await self.broadcast()
        elif kind == "motion":
            player["motion"] = min(1.0, max(0.0, float(msg.get("v", 0))))
            await self.broadcast()


def clean(text, limit):
    text = "".join(ch for ch in str(text) if ch.isprintable()).strip()
    return text[:limit]


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    room = player = None
    try:
        async for raw in ws:
            if raw.type == WSMsgType.BINARY:
                # A thermal frame: pass it on to everyone else, tagged with the sender's id
                if room is not None and len(raw.data) <= MAX_FRAME:
                    packet = struct.pack(">I", player["id"]) + raw.data
                    for other in list(room.players):
                        if other is not ws and not other.closed:
                            try:
                                await other.send_bytes(packet)
                            except Exception:
                                pass
                continue
            if raw.type != WSMsgType.TEXT:
                continue
            try:
                msg = raw.json()
            except ValueError:
                continue
            if room is None:
                if msg.get("t") != "join":
                    continue
                code = re.sub(r"[^a-z0-9]+", "-", str(msg.get("room", "")).lower()).strip("-")[:24]
                if not code:
                    continue
                room = rooms.get(code) or rooms.setdefault(code, Room(code))
                if len(room.players) >= MAX_PLAYERS:
                    await ws.send_json({"t": "error", "text": "This room is full (%d players)." % MAX_PLAYERS})
                    room = None
                    break
                player = {"id": room.next_id, "name": clean(msg.get("name", ""), 20) or "Player %d" % room.next_id,
                          "motion": 0, "torch": 0, "done": False, "flagged": False}
                room.next_id += 1
                room.players[ws] = player
                await room.broadcast()
                continue
            await room.on_message(player, msg)
    finally:
        if room is not None and ws in room.players:
            del room.players[ws]
            if not room.players:
                if room.task:
                    room.task.cancel()
                rooms.pop(room.code, None)
            else:
                await room.broadcast()
    return ws


async def index(request):
    return web.FileResponse(os.path.join(STATIC, "index.html"))


async def clip(request):
    path = os.path.join(STATIC, "clip.mp4")
    if not os.path.exists(path):
        raise web.HTTPNotFound()
    return web.FileResponse(path)


def main():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/clip.mp4", clip)
    web.run_app(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8642)))


if __name__ == "__main__":
    main()
