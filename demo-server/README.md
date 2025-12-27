# CRDT Rogue

A multiplayer rogue-like survival game built to demonstrate the power of **Conflict-free Replicated Data Types (CRDTs)**.

## How It Works

The game server uses a **Sharded LWWMap (Last-Write-Wins Map)** to manage the state of the world. 
- **State**: Every entity (Player, Monster, Coin, Sword) is an entry in the CRDT map.
- **Concurrency**: 16 parallel shards process updates independently, allowing for high-throughput state merging.
- **Sync**: Clients send optimistic updates via WebSockets. The server merges them using CRDT logic and broadcasts the result.

## Gameplay

Survive as long as you can against the endless horde of monsters!

### Controls
- **Arrow Keys / WASD**: Move your character (`@`).
- **Click**: Respawn if you die.

### Mechanics
- **Health**: You start with 3 Hearts (Green). Taking damage reduces your health and changes your color (Amber -> Red).
- **Combat**: 
  - Walk into a **Sword** (`†`) to pick it up.
  - While holding a sword (Cyan Color), walk into a **Monster** (`M`) to kill it.
  - Swords have limited durability (5 hits).
- **Scoring**:
  - Collect **Coins** (`o`) for +10 points.
  - Kill **Monsters** for +50 points.
- **Progression**:
  - As you collect more coins, the monster spawn rate increases!
  - Swords are rare items (max 5 on the map at once).

## Running the Game

1. **Start the Server**:
   ```bash
   ./run-demo-server.sh
   ```
   Or manually:
   ```bash
   cargo run -p demo-server
   ```

2. **Play**:
   Open your browser to [http://localhost:3000](http://localhost:3000).
   Open multiple tabs to simulate multiplayer!

## Tech Stack
- **Backend**: Rust, Axum, Tokio
- **Data Structure**: `crdt-data-types` (LWWMap)
- **Frontend**: HTML5 Canvas, Vanilla JS, WebSockets
