import asyncio
import json
import uuid
import random
import os
import logging
import websockets

# Configure logging for Render Dashboard
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Port configuration for Render
PORT = int(os.environ.get("PORT", 8000))

# =========================================================================
# 1. TIC-TAC-TOE STATE & LOGIC
# =========================================================================
ttt_queues = {} # Dynamically generates matching buckets for X and O
ttt_rooms = {}
ttt_player_state = {} 

class TicTacToeRoom:
    def __init__(self, room_id, size, infinity, blitz, p1_ws, p2_ws):
        self.room_id = room_id
        self.board_size = size
        self.infinity = infinity
        self.blitz = blitz
        
        # p1_ws is ALWAYS X, p2_ws is ALWAYS O
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
    assigned_queue_key = None
    assigned_symbol = None
    try:
        async for msg in websocket:
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                continue

            action = data.get("action")

            if action == "join_queue":
                size = data.get("board_size", 3)
                inf = data.get("infinity", 0)
                blitz = data.get("blitz", 0)
                symbol = data.get("symbol", "X") 
                
                assigned_queue_key = f"{size}:{inf}:{blitz}"
                assigned_symbol = symbol
                opp_symbol = 'O' if symbol == 'X' else 'X'
                
                if assigned_queue_key not in ttt_queues:
                    ttt_queues[assigned_queue_key] = {'X': [], 'O': []}
                    
                queue = ttt_queues[assigned_queue_key]

                if len(queue[opp_symbol]) > 0:
                    opp_ws = queue[opp_symbol].pop(0)
                    my_ws = websocket
                    room_id = str(uuid.uuid4())
                    
                    p1 = my_ws if symbol == 'X' else opp_ws
                    p2 = my_ws if symbol == 'O' else opp_ws
                    
                    new_room = TicTacToeRoom(room_id, size, inf, blitz, p1, p2)
                    ttt_rooms[room_id] = new_room
                    
                    ttt_player_state[my_ws] = {"room_id": room_id, "symbol": symbol}
                    ttt_player_state[opp_ws] = {"room_id": room_id, "symbol": opp_symbol}

                    await my_ws.send(json.dumps({"action": "match_start", "room_id": room_id, "assigned_symbol": symbol, "current_turn": "X"}))
                    await opp_ws.send(json.dumps({"action": "match_start", "room_id": room_id, "assigned_symbol": opp_symbol, "current_turn": "X"}))
                    logging.info(f"Tic-Tac-Toe: Match started. Room ID: {room_id}")
                    
                    await new_room.start_blitz_countdown()
                else:
                    if websocket not in queue[symbol]:
                        queue[symbol].append(websocket)
                    await websocket.send(json.dumps({"action": "queued"}))

            elif action == "submit_move":
                state = ttt_player_state.get(websocket)
                if state:
                    room_id = state["room_id"]
                    symbol = state["symbol"]
                    room = ttt_rooms.get(room_id)
                    if room:
                        await room.process_move(symbol, data.get("index"))
                        
            elif action == "ping":
                await websocket.send(json.dumps({"action": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logging.error(f"Tic-Tac-Toe Error: {e}")
    finally:
        # 1. Safely clean queue
        if assigned_queue_key and assigned_symbol:
            queue = ttt_queues.get(assigned_queue_key)
            if queue and websocket in queue[assigned_symbol]:
                queue[assigned_symbol].remove(websocket)
                
        # 2. Safely clean room and state using .pop() to prevent KeyErrors
        state = ttt_player_state.get(websocket)
        if state:
            room_id = state["room_id"]
            room = ttt_rooms.pop(room_id, None)
            if room:
                await room.terminate_on_disconnect(websocket)
                logging.info(f"Tic-Tac-Toe: Room {room_id} closed due to disconnect.")
            ttt_player_state.pop(websocket, None)

# =========================================================================
# 2. CHECKERBOARD MULTIPLAYER STATE & LOGIC
# =========================================================================
cb_queues = {} 
cb_rooms = {}
cb_player_state = {}

class CheckerboardRoom:
    def __init__(self, room_id, p1_ws, p2_ws):
        self.room_id = room_id
        self.players = {1: p1_ws, 2: p2_ws}
        self.current_turn = 1

    async def process_move(self, color, start, end):
        if color != self.current_turn: 
            return
        
        self.current_turn = 2 if color == 1 else 1
        other_color = self.current_turn
        
        try:
            await self.players[other_color].send(json.dumps({
                "action": "state_update",
                "start": start,
                "end": end
            }))
        except Exception as e:
            logging.error(f"Checkerboard move relay failed: {e}")

    async def terminate_on_disconnect(self, disconnected_ws):
        remaining_color = 2 if self.players[1] == disconnected_ws else 1
        try:
            await self.players[remaining_color].send(json.dumps({
                "action": "opponent_disconnected"
            }))
        except Exception:
            pass

async def checkerboard_logic(websocket):
    assigned_queue_key = None
    assigned_color = None
    
    try:
        async for msg in websocket:
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                continue

            action = data.get("action")

            if action == "join_queue":
                color = data.get("color", 1)
                m_jump = data.get("mandatory_jump", True)
                blitz = data.get("blitz", "OFF")
                
                queue_key = f"{m_jump}:{blitz}"
                if queue_key not in cb_queues:
                    cb_queues[queue_key] = {1: [], 2: []}
                    
                assigned_queue_key = queue_key
                assigned_color = color
                opp_color = 2 if color == 1 else 1
                queue = cb_queues[queue_key]
                
                if len(queue[opp_color]) > 0:
                    opp_ws = queue[opp_color].pop(0)
                    my_ws = websocket
                    
                    room_id = str(uuid.uuid4())
                    new_room = CheckerboardRoom(room_id, my_ws if color==1 else opp_ws, my_ws if color==2 else opp_ws)
                    cb_rooms[room_id] = new_room
                    
                    cb_player_state[my_ws] = {"room_id": room_id, "color": color}
                    cb_player_state[opp_ws] = {"room_id": room_id, "color": opp_color}
                    
                    await my_ws.send(json.dumps({"action": "match_start", "color": color}))
                    await opp_ws.send(json.dumps({"action": "match_start", "color": opp_color}))
                    logging.info(f"Checkerboard: Match started in '{queue_key}'. Room ID: {room_id}")
                else:
                    if websocket not in queue[color]:
                        queue[color].append(websocket)
                        logging.info(f"Checkerboard: Player joined queue '{queue_key}' as Color {color}")

            elif action == "submit_move":
                state = cb_player_state.get(websocket)
                if state:
                    room = cb_rooms.get(state["room_id"])
                    if room:
                        await room.process_move(state["color"], data.get("start"), data.get("end"))

            elif action == "ping":
                await websocket.send(json.dumps({"action": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logging.error(f"Checkerboard Error: {e}")
    finally:
        # 1. Safely clean queue
        if assigned_queue_key and assigned_color:
            queue = cb_queues.get(assigned_queue_key)
            if queue and websocket in queue[assigned_color]:
                queue[assigned_color].remove(websocket)
                logging.info(f"Checkerboard: Player left queue '{assigned_queue_key}'")
            
        # 2. Safely clean room and state using .pop() to prevent KeyErrors
        state = cb_player_state.get(websocket)
        if state:
            room_id = state["room_id"]
            room = cb_rooms.pop(room_id, None)
            if room:
                await room.terminate_on_disconnect(websocket)
                logging.info(f"Checkerboard: Room {room_id} closed due to disconnect.")
            cb_player_state.pop(websocket, None)

# =========================================================================
# 3. MASTER CONNECTION ROUTER
# =========================================================================
async def connection_router(websocket, path=None):
    if hasattr(websocket, 'request'):
        req_path = websocket.request.path 
    else:
        req_path = path or getattr(websocket, 'path', '/')
        
    logging.info(f"Incoming connection to: {req_path}")
    
    if req_path == "/tictactoe":
        await tictactoe_logic(websocket)
    elif req_path == "/checkerboard":
        await checkerboard_logic(websocket)
    else:
        logging.warning(f"Rejected unauthorized path: {req_path}")
        await websocket.close(code=1008, reason="Invalid game path requested.")

# =========================================================================
# 4. SERVER INITIALIZATION
# =========================================================================
async def main():
    logging.info(f"Master Multi-Game Server booting up on port {PORT}...")
    # Added native ping_interval=20 to prevent Render 55s timeout disconnects
    async with websockets.serve(connection_router, "0.0.0.0", PORT, ping_interval=20, ping_timeout=20):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
