import asyncio
import json
import uuid
import random
import os
import websockets

# Port configuration for Render
PORT = int(os.environ.get("PORT", 8000))

# =========================================================================
# 1. TIC-TAC-TOE STATE & LOGIC
# =========================================================================
ttt_queues = {
    "3:0:0": [], "3:1:0": [], "3:0:1": [], "3:1:1": [],
    "5:0:0": [], "5:1:0": [], "5:0:1": [], "5:1:1": []
}
ttt_rooms = {}
ttt_player_state = {}  # SAFE STATE TRACKER: Maps a websocket to its room_id and symbol

class TicTacToeRoom:
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
            await asyncio.sleep(5.5) 
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


async def tictactoe_logic(websocket):
    """ Handles all connections routed to /tictactoe """
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
                
                queue = ttt_queues[assigned_queue_key]
                if websocket not in queue:
                    queue.append(websocket)

                if len(queue) >= 2:
                    p1 = queue.pop(0)
                    p2 = queue.pop(0)
                    room_id = str(uuid.uuid4())
                    
                    new_room = TicTacToeRoom(room_id, size, inf, blitz, p1, p2)
                    ttt_rooms[room_id] = new_room
                    
                    # Safe dictionary tracking instead of modifying websocket.__slots__
                    ttt_player_state[p1] = {"room_id": room_id, "symbol": "X"}
                    ttt_player_state[p2] = {"room_id": room_id, "symbol": "O"}

                    await p1.send(json.dumps({"action": "match_start", "room_id": room_id, "assigned_symbol": "X", "current_turn": "X"}))
                    await p2.send(json.dumps({"action": "match_start", "room_id": room_id, "assigned_symbol": "O", "current_turn": "X"}))
                    
                    await new_room.start_blitz_countdown()
                else:
                    await websocket.send(json.dumps({"action": "queued"}))

            elif action == "submit_move":
                state = ttt_player_state.get(websocket)
                if state:
                    room_id = state["room_id"]
                    symbol = state["symbol"]
                    if room_id in ttt_rooms:
                        await ttt_rooms[room_id].process_move(symbol, data.get("index"))
                        
            elif action == "ping":
                await websocket.send(json.dumps({"action": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"Tic-Tac-Toe Error: {e}")
    finally:
        # 1. Remove from queue if they disconnect while searching
        if assigned_queue_key and websocket in ttt_queues[assigned_queue_key]:
            ttt_queues[assigned_queue_key].remove(websocket)
            
        # 2. End match gracefully if they disconnect mid-game
        state = ttt_player_state.get(websocket)
        if state:
            room_id = state["room_id"]
            if room_id in ttt_rooms:
                room = ttt_rooms[room_id]
                await room.terminate_on_disconnect(websocket)
                del ttt_rooms[room_id]
            # Delete their tracker
            del ttt_player_state[websocket]

# =========================================================================
# 2. CHECKERBOARD STATE & LOGIC (PLACEHOLDER)
# =========================================================================
checkerboard_queues = {}
checkerboard_rooms = {}

async def checkerboard_logic(websocket):
    """ Handles all connections routed to /checkerboard """
    try:
        print("A player joined the Checkerboard lobby!")
        await websocket.send(json.dumps({"action": "connected", "message": "Checkerboard server is alive!"}))
        
        async for msg in websocket:
            data = json.loads(msg)
            # Future Checkerboard logic will go here
            pass
            
    except websockets.exceptions.ConnectionClosed:
        print("A player left the Checkerboard lobby.")
    except Exception as e:
        print(f"Checkerboard Error: {e}")

# =========================================================================
# 3. MASTER CONNECTION ROUTER
# =========================================================================
async def connection_router(websocket, path=None):
    """ Intercepts all incoming connections and routes them safely by URL """
    
    if hasattr(websocket, 'request'):
        req_path = websocket.request.path 
    else:
        req_path = path or getattr(websocket, 'path', '/')
        
    print(f"Incoming connection to: {req_path}")
    
    if req_path == "/tictactoe":
        await tictactoe_logic(websocket)
        
    elif req_path == "/checkerboard":
        await checkerboard_logic(websocket)
        
    else:
        print(f"Rejected unauthorized path: {req_path}")
        await websocket.close(code=1008, reason="Invalid game path requested.")

# =========================================================================
# 4. SERVER INITIALIZATION
# =========================================================================
async def main():
    print(f"Master Multi-Game Server booting up on port {PORT}...")
    async with websockets.serve(connection_router, "0.0.0.0", PORT):
        await asyncio.Future()  # Keep server running forever

if __name__ == "__main__":
    asyncio.run(main())
