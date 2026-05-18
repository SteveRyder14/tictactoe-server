import asyncio
import json
import uuid
import random
import os
import websockets

# Port configuration for Render
PORT = int(os.environ.get("PORT", 8000))

# Queues mapped by unique combination keys: "board_size:infinity:blitz"
matchmaking_queues = {
    "3:0:0": [], "3:1:0": [], "3:0:1": [], "3:1:1": [],
    "5:0:0": [], "5:1:0": [], "5:0:1": [], "5:1:1": []
}
active_rooms = {}

class Room:
    def __init__(self, room_id, size, infinity, blitz, p1_ws, p2_ws):
        self.room_id = room_id
        self.board_size = size
        self.infinity = infinity
        self.blitz = blitz
        
        self.players = {'X': p1_ws, 'O': p2_ws}
        self.current_turn = 'X'
        self.board = [""] * (size * size)
        self.game_over = False
        self.pieces = {'X': [], 'O': []}
        self.timer_task = None
        self.win_lines = self.get_lines(size)

    def get_lines(self, size):
        if size == 3:
            return [[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]]
        else:
            return [
                [0, 1, 2, 3], [1, 2, 3, 4], [5, 6, 7, 8], [6, 7, 8, 9], [10, 11, 12, 13], 
                [11, 12, 13, 14], [15, 16, 17, 18], [16, 17, 18, 19], [20, 21, 22, 23], [21, 22, 23, 24],
                [0, 5, 10, 15], [5, 10, 15, 20], [1, 6, 11, 16], [6, 11, 16, 21], [2, 7, 12, 17], 
                [7, 12, 17, 22], [3, 8, 13, 18], [8, 13, 18, 23], [4, 9, 14, 19], [9, 14, 19, 24],
                [0, 6, 12, 18], [6, 12, 18, 24], [1, 7, 12, 17], [2, 8, 14, 20], [5, 11, 17, 23],
                [3, 7, 11, 15], [4, 8, 12, 16], [8, 12, 16, 20], [9, 13, 17, 21], [14, 18, 22, 24]
            ]

    async def broadcast(self, payload):
        message = json.dumps(payload)
        tasks = [asyncio.create_task(ws.send(message)) for ws in self.players.values()]
        if tasks:
            await asyncio.wait(tasks)

    def cancel_timer(self):
        if self.timer_task:
            self.timer_task.cancel()
            self.timer_task = None

    async def start_blitz_countdown(self):
        self.cancel_timer()
        if not self.blitz or self.game_over:
            return
        self.timer_task = asyncio.create_task(self._timer_worker())

    async def _timer_worker(self):
        try:
            await asyncio.sleep(5.5) # Server-side leeway for network lag
            available = [i for i, val in enumerate(self.board) if val == ""]
            if available and not self.game_over:
                random_idx = random.choice(available)
                await self.process_move(self.current_turn, random_idx)
        except asyncio.CancelledError:
            pass

    async def process_move(self, symbol, index):
        if self.game_over or self.current_turn != symbol or self.board[index] != "":
            return

        self.cancel_timer()
        removed_index = None
        self.board[index] = symbol
        self.pieces[symbol].append(index)

        if self.infinity:
            max_pieces = 3 if self.board_size == 3 else 4
            if len(self.pieces[symbol]) > max_pieces:
                removed_index = self.pieces[symbol].pop(0)
                self.board[removed_index] = ""

        next_player = 'O' if symbol == 'X' else 'X'
        self.current_turn = next_player

        await self.broadcast({
            "action": "state_update",
            "placed_index": index,
            "placed_symbol": symbol,
            "removed_index": removed_index,
            "next_turn": next_player
        })

        if await self.check_end_conditions():
            return

        await self.start_blitz_countdown()

    async def check_end_conditions(self):
        for line in self.win_lines:
            vals = [self.board[idx] for idx in line]
            if vals[0] != "" and vals.count(vals[0]) == len(line):
                self.game_over = True
                await self.broadcast({
                    "action": "game_over",
                    "winner": vals[0],
                    "winning_line": line,
                    "reason": "win"
                })
                return True

        if "" not in self.board:
            self.game_over = True
            await self.broadcast({
                "action": "game_over",
                "winner": None,
                "winning_line": [],
                "reason": "tie"
            })
            return True
        return False

    async def terminate_on_disconnect(self, disconnected_ws):
        self.game_over = True
        self.cancel_timer()
        remaining_symbol = 'O' if self.players['X'] == disconnected_ws else 'X'
        try:
            await self.players[remaining_symbol].send(json.dumps({
                "action": "game_over",
                "winner": remaining_symbol,
                "winning_line": [],
                "reason": "disconnect"
            }))
        except Exception:
            pass


async def handler(websocket):
    assigned_room_id = None
    assigned_queue_key = None
    
    try:
        async for msg in websocket:
            data = json.loads(msg)
            action = data.get("action")

            if action == "join_queue":
                size = data.get("board_size", 3)
                inf = data.get("infinity", 0)
                blitz = data.get("blitz", 0)
                assigned_queue_key = f"{size}:{inf}:{blitz}"
                
                queue = matchmaking_queues[assigned_queue_key]
                if websocket not in queue:
                    queue.append(websocket)

                if len(queue) >= 2:
                    p1 = queue.pop(0)
                    p2 = queue.pop(0)
                    room_id = str(uuid.uuid4())
                    
                    new_room = Room(room_id, size, inf, blitz, p1, p2)
                    active_rooms[room_id] = new_room
                    
                    p1.room_id = room_id; p1.symbol = 'X'
                    p2.room_id = room_id; p2.symbol = 'O'
                    assigned_room_id = room_id

                    await p1.send(json.dumps({"action": "match_start", "room_id": room_id, "assigned_symbol": "X", "current_turn": "X"}))
                    await p2.send(json.dumps({"action": "match_start", "room_id": room_id, "assigned_symbol": "O", "current_turn": "X"}))
                    
                    await new_room.start_blitz_countdown()
                else:
                    await websocket.send(json.dumps({"action": "queued"}))

            elif action == "submit_move":
                room_id = getattr(websocket, 'room_id', None)
                symbol = getattr(websocket, 'symbol', None)
                if room_id in active_rooms:
                    await active_rooms[room_id].process_move(symbol, data.get("index"))

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if assigned_queue_key and websocket in matchmaking_queues[assigned_queue_key]:
            matchmaking_queues[assigned_queue_key].remove(websocket)
            
        room_id = getattr(websocket, 'room_id', None)
        if room_id in active_rooms:
            room = active_rooms[room_id]
            await room.terminate_on_disconnect(websocket)
            if room_id in active_rooms:
                del active_rooms[room_id]


async def main():
    print(f"Starting server on port {PORT}...")
    async with websockets.serve(handler, "0.0.0.0", PORT, ping_interval=10, ping_timeout=10):
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())