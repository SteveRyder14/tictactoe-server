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
ttt_queues = {
    "3:0:0": [], "3:1:0": [], "3:0:1": [], "3:1:1": [],
    "5:0:0": [], "5:1:0": [], "5:0:1": [], "5:1:1": []
}
ttt_rooms = {}
ttt_player_state = {}  

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
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                continue

            action = data.get("action")

            if action == "join_queue":
                size = data.get("board_size", 3)
                inf = data.get("infinity", 0)
                blitz = data.get("blitz", 0)
                assigned_queue_key = f"{size}:{inf}:{blitz}"
                
                queue = ttt_queues[assigned_queue_key]
                if websocket not in queue:
                    queue.append(websocket)
                    logging.info(f"Tic-Tac-Toe: Player joined queue '{assigned_queue_key}'")

                if len(queue) >= 2:
                    p1 = queue.pop(0)
                    p2 = queue.pop(0)
                    room_id = str(uuid.uuid4())
                    
                    new_room = TicTacToeRoom(room_id, size, inf, blitz, p1, p2)
                    ttt_rooms[room_id] = new_room
                    
                    ttt_player_state[p1] = {"room_id": room_id, "symbol": "X"}
                    ttt_player_state[p2] = {"room_id": room_id, "symbol": "O"}

                    await p1.send(json.dumps({"action": "match_start", "room_id": room_id, "assigned_symbol": "X", "current_turn": "X"}))
                    await p2.send(json.dumps({"action": "match_start", "room_id": room_id, "assigned_symbol": "O", "current_turn": "X"}))
                    logging.info(f"Tic-Tac-Toe: Match started. Room ID: {room_id}")
                    
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
        logging.error(f"Tic-Tac-Toe Error: {e}")
    finally:
        if assigned_queue_key and websocket in ttt_queues.get(assigned_queue_key, []):
            ttt_queues[assigned_queue_key].remove(websocket)
            logging.info("Tic-Tac-Toe: Player left queue.")
            
        state = ttt_player_state.get(websocket)
        if state:
            room_id = state["room_id"]
            if room_id in ttt_rooms:
                room = ttt_rooms[room_id]
                await room.terminate_on_disconnect(websocket)
                del ttt_rooms[room_id]
                logging.info(f"Tic-Tac-Toe: Room {room_id} closed due to disconnect.")
            
            if websocket in ttt_player_state:
                del ttt_player_state[websocket]

# =========================================================================
# 2. CHECKERBOARD MULTIPLAYER STATE & LOGIC
# =========================================================================
cb_queue = []
cb_rooms = {}
cb_player_state = {}

class CheckerboardRoom:
    def __init__(self, room_id, p1_ws, p2_ws):
        self.room_id = room_id
        self.players = {1: p1_ws, 2: p2_ws}
        self.current_turn = 1

    async def process_move(self, color, start, end):
        # Validate turn sync to prevent cheating/double moves
        if color != self.current_turn: 
            return
        
        # Switch turn
        self.current_turn = 2 if color == 1 else 1
        
        # Relay move securely to the OPPONENT
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
    """ Handles all connections routed to /checkerboard """
    assigned_queue = False
    try:
        async for msg in websocket:
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                continue

            action = data.get("action")

            if action == "join_queue":
                if websocket not in cb_queue:
                    cb_queue.append(websocket)
                    assigned_queue = True
                    logging.info(f"Checkerboard: Player joined queue. Waiting: {len(cb_queue)}")
                
                # Matchmaker
                if len(cb_queue) >= 2:
                    p1 = cb_queue.pop(0)
                    p2 = cb_queue.pop(0)
                    room_id = str(uuid.uuid4())
                    
                    new_room = CheckerboardRoom(room_id, p1, p2)
                    cb_rooms[room_id] = new_room
                    
                    # 1 = BLUE (Bottom/First), 2 = RED (Top/Second)
                    cb_player_state[p1] = {"room_id": room_id, "color": 1}
                    cb_player_state[p2] = {"room_id": room_id, "color": 2}

                    await p1.send(json.dumps({"action": "match_start", "color": 1}))
                    await p2.send(json.dumps({"action": "match_start", "color": 2}))
                    logging.info(f"Checkerboard: Match started. Room ID: {room_id}")

            elif action == "submit_move":
                state = cb_player_state.get(websocket)
                if state:
                    room = cb_rooms.get(state["room_id"])
                    if room:
                        await room.process_move(state["color"], data.get("start"), data.get("end"))

            # Keep-Alive for Render's 55s timeout
            elif action == "ping":
                await websocket.send(json.dumps({"action": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logging.error(f"Checkerboard Error: {e}")
    finally:
        if assigned_queue and websocket in cb_queue:
            cb_queue.remove(websocket)
            logging.info("Checkerboard: Player left queue.")
            
        state = cb_player_state.get(websocket)
        if state:
            room_id = state["room_id"]
            if room_id in cb_rooms:
                await cb_rooms[room_id].terminate_on_disconnect(websocket)
                del cb_rooms[room_id]
                logging.info(f"Checkerboard: Room {room_id} closed due to disconnect.")
            if websocket in cb_player_state:
                del cb_player_state[websocket]

# =========================================================================
# 3. MASTER CONNECTION ROUTER
# =========================================================================
async def connection_router(websocket, path=None):
    """ Intercepts all incoming connections and routes them safely by URL """
    
    if hasattr(websocket, 'request'):
        req_path = websocket.request.path 
    else:
        req_path = path or getattr(websocket, 'path', '/')
        
    logging.info(f"Incoming connection routed to: {req_path}")
    
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
    async with websockets.serve(connection_router, "0.0.0.0", PORT):
        await asyncio.Future()  # Keep server running forever

if __name__ == "__main__":
    asyncio.run(main())
