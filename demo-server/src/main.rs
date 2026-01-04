use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use crdt_data_types::LWWMap;
use futures::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::{atomic::{AtomicU64, Ordering}, Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, mpsc};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use rand::Rng;

// ============================================================================
// Constants & Config
// ============================================================================

const NUM_LANES: usize = 16;
const MAP_WIDTH: i32 = 40;
const MAP_HEIGHT: i32 = 30;
const TICK_RATE_MS: u64 = 200;
const MAX_MONSTERS: usize = 50;

// ============================================================================
// Game Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
enum EntityKind {
    Player,
    Monster,
    Coin,
    Sword,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Entity {
    id: String,
    x: i32,
    y: i32,
    kind: EntityKind,
    color: String,
    score: u32,      // Only relevant for players
    durability: u32, // For players (weapon charges) or items
    spawn_time: u64, // For aging mechanics
    health: u32,     // For players
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GameUpdate {
    entity: Entity,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatsUpdate {
    stats: ServerStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServerStats {
    events_processed: u64,
    active_players: usize,
    active_monsters: usize,
}

// ============================================================================
// App State
// ============================================================================

struct AppState {
    // Broadcast channel for pushing updates to all connected clients
    tx: broadcast::Sender<String>,
    // Channels to send updates to specific lanes
    lane_channels: Vec<mpsc::Sender<GameUpdate>>,
    // Shared state for the game loop to read (Lanes write to this)
    shards: Vec<Arc<RwLock<LWWMap<String, Entity>>>>,
    // Global event counter
    events_processed: Arc<AtomicU64>,
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "demo_server=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // 1. Setup Broadcast Channel (Server -> Clients)
    let (tx, _rx) = broadcast::channel(10000);

    // 2. Setup Lanes (Ingest -> State)
    let mut lane_channels = Vec::with_capacity(NUM_LANES);
    let mut shards = Vec::with_capacity(NUM_LANES);
    let events_processed = Arc::new(AtomicU64::new(0));

    for i in 0..NUM_LANES {
        let (lane_tx, mut lane_rx) = mpsc::channel::<GameUpdate>(10000);
        lane_channels.push(lane_tx);
        
        let shard = Arc::new(RwLock::new(LWWMap::new()));
        shards.push(shard.clone());
        
        let broadcast_tx = tx.clone();
        let shard_clone = shard.clone();
        let events_clone = events_processed.clone();
        
        tokio::spawn(async move {
            tracing::info!("Lane {} started", i);
            
            while let Some(update) = lane_rx.recv().await {
                // 1. Update Local CRDT State
                {
                    let mut map = shard_clone.write().unwrap();
                    if update.entity.kind == EntityKind::Deleted {
                        map.remove(&update.entity.id);
                        tracing::info!(
                            "Lane {}: [CRDT REMOVE] Entity {} deleted. Timestamp: {}", 
                            i, update.entity.id, update.timestamp
                        );
                    } else {
                        // Check if this is an update or new insert
                        let exists = map.entries.iter().any(|(k, _)| k == &update.entity.id);
                        let action = if exists { "UPDATE" } else { "INSERT" };
                        
                        map.insert(
                            "server",
                            update.entity.id.clone(),
                            update.entity.clone(),
                            update.timestamp
                        );
                        
                        tracing::info!(
                            "Lane {}: [CRDT {}] Entity {} ({:?}) merged. Timestamp: {}", 
                            i, action, update.entity.id, update.entity.kind, update.timestamp
                        );
                    }
                }
                
                events_clone.fetch_add(1, Ordering::Relaxed);

                // 2. Broadcast to clients
                if let Ok(msg) = serde_json::to_string(&update) {
                    let _ = broadcast_tx.send(msg);
                }
            }
        });
    }

    let app_state = Arc::new(AppState {
        tx,
        lane_channels: lane_channels.clone(),
        shards: shards.clone(),
        events_processed: events_processed.clone(),
    });

    // 3. Start Game Loop (AI & Spawning)
    let game_state = app_state.clone();
    tokio::spawn(async move {
        game_loop(game_state).await;
    });

    // 4. Setup Router
    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(ws_handler))
        .with_state(app_state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("Game Server listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ============================================================================
// Game Loop
// ============================================================================

async fn game_loop(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_millis(TICK_RATE_MS));
    let mut tick_count: u64 = 0;
    
    loop {
        interval.tick().await;
        tick_count += 1;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        // 1. Move Monsters & Check Collisions
        // We iterate all shards to find monsters and players
        let mut players = Vec::new();
        let mut coins = Vec::new();
        let mut swords = Vec::new();
        let mut monsters = Vec::new();
        
        for shard in &state.shards {
            let map = shard.read().unwrap();
            for (_k, (v, _ts, _node)) in &map.entries {
                match v.kind {
                    EntityKind::Player => players.push(v.clone()),
                    EntityKind::Coin => coins.push(v.clone()),
                    EntityKind::Sword => swords.push(v.clone()),
                    EntityKind::Monster => monsters.push(v.clone()),
                    _ => {}
                }
            }
        }

        // 0. Cleanup if no players
        if players.is_empty() && (!monsters.is_empty() || !coins.is_empty() || !swords.is_empty()) {
            tracing::info!("No players online. Wiping world state.");
            for entity in monsters.iter().chain(coins.iter()).chain(swords.iter()) {
                let mut deleted = entity.clone();
                deleted.kind = EntityKind::Deleted;
                route_update(GameUpdate { entity: deleted, timestamp: now }, &state.lane_channels).await;
            }
            // Clear local vecs so we don't process them this tick
            monsters.clear();
            coins.clear();
            swords.clear();
        }

        // Check Collisions
        let mut killed_monster_ids = std::collections::HashSet::new();
        for player in &players {
            let mut current_player = player.clone();
            let mut player_changed = false;

            // Player vs Coin
            for coin in &coins {
                if current_player.x == coin.x && current_player.y == coin.y {
                    // Pickup Coin
                    let mut deleted_coin = coin.clone();
                    deleted_coin.kind = EntityKind::Deleted;
                    route_update(GameUpdate { entity: deleted_coin, timestamp: now }, &state.lane_channels).await;
                    
                    // Update Player Score
                    current_player.score += 10;
                    player_changed = true;
                }
            }

            // Player vs Sword
            for sword in &swords {
                if current_player.x == sword.x && current_player.y == sword.y {
                    // Pickup Sword
                    let mut deleted_sword = sword.clone();
                    deleted_sword.kind = EntityKind::Deleted;
                    route_update(GameUpdate { entity: deleted_sword, timestamp: now }, &state.lane_channels).await;
                    
                    // Update Player Weapon
                    current_player.durability = current_player.durability.saturating_add(5); // Stack durability
                    current_player.color = "#00FFFF".to_string(); // Cyan power-up
                    tracing::info!("Player {} picked up sword. Durability: {}", current_player.id, current_player.durability);
                    player_changed = true;
                }
            }

            // Player vs Monster
            for monster in &monsters {
                if killed_monster_ids.contains(&monster.id) {
                    continue;
                }
                if current_player.x == monster.x && current_player.y == monster.y {
                    if current_player.durability > 0 {
                        // Kill Monster
                        let mut deleted_monster = monster.clone();
                        deleted_monster.kind = EntityKind::Deleted;
                        route_update(GameUpdate { entity: deleted_monster, timestamp: now }, &state.lane_channels).await;
                        killed_monster_ids.insert(monster.id.clone());

                        // Update Player
                        current_player.durability -= 1;
                        current_player.score += 50;
                        
                        // Update Color
                        if current_player.durability == 0 {
                            current_player.color = match current_player.health {
                                3 => "#00FF00".to_string(), // Green
                                2 => "#FFA500".to_string(), // Orange
                                _ => "#FF0000".to_string(), // Red
                            };
                        }
                        player_changed = true;
                    } else {
                        // Player Takes Damage
                        if current_player.health > 1 {
                            current_player.health -= 1;
                            
                            // Update Color
                            current_player.color = match current_player.health {
                                3 => "#00FF00".to_string(), // Green
                                2 => "#FFA500".to_string(), // Orange
                                _ => "#FF0000".to_string(), // Red
                            };
                            
                            tracing::info!("Player {} took damage. Health: {}", current_player.id, current_player.health);
                            player_changed = true;
                        } else {
                            // Player Dies
                            let mut dead_player = current_player.clone();
                            dead_player.kind = EntityKind::Deleted;
                            route_update(GameUpdate { entity: dead_player, timestamp: now }, &state.lane_channels).await;
                            player_changed = false; // Already sent update
                            break; // Stop processing this player
                        }
                    }
                }
            }

            if player_changed {
                route_update(GameUpdate { entity: current_player, timestamp: now }, &state.lane_channels).await;
            }
        }

        // Move Monsters (Dynamic speed based on age)
        // We iterate all monsters every tick, but only move them based on probability
        if tick_count % 5 == 0 {
            // Broadcast Stats
            let stats = StatsUpdate {
                stats: ServerStats {
                    events_processed: state.events_processed.load(Ordering::Relaxed),
                    active_players: players.len(),
                    active_monsters: monsters.len(),
                }
            };
            if let Ok(msg) = serde_json::to_string(&stats) {
                let _ = state.tx.send(msg);
            }
        }

        // We need to clone monsters for the movement loop because we need the count later for sword spawning
        let monsters_for_movement = monsters.clone();

        for mut monster in monsters_for_movement {
            // Calculate move chance based on age
            // Base: 20% (every 5 ticks)
            // Max: 60% (every ~1.6 ticks)
            // Age factor: +2% per second alive
            let age_seconds = (now - monster.spawn_time) / 1000;
            let move_chance = 0.2 + (age_seconds as f64 * 0.02);
            
            if !rand::rng().random_bool(move_chance.clamp(0.2, 0.6)) {
                continue;
            }

            // Find nearest player
            let mut target_x = monster.x;
            let mut target_y = monster.y;
            let mut min_dist = f32::MAX;
            
            for player in &players {
                let dist = (((player.x - monster.x).pow(2) + (player.y - monster.y).pow(2)) as f32).sqrt();
                if dist < min_dist {
                    min_dist = dist;
                    target_x = player.x;
                    target_y = player.y;
                }
            }

            // Move towards target
            let dx = (target_x - monster.x).signum();
            let dy = (target_y - monster.y).signum();
            
            // Add some randomness so they don't stack perfectly
            let random_move = rand::rng().random_bool(0.2);
            
            if min_dist < 15.0 && !random_move { // Only chase if within range
                monster.x = (monster.x + dx).clamp(0, MAP_WIDTH - 1);
                monster.y = (monster.y + dy).clamp(0, MAP_HEIGHT - 1);
            } else {
                // Random wander
                let rdx = rand::rng().random_range(-1..=1);
                let rdy = rand::rng().random_range(-1..=1);
                monster.x = (monster.x + rdx).clamp(0, MAP_WIDTH - 1);
                monster.y = (monster.y + rdy).clamp(0, MAP_HEIGHT - 1);
            }
            
            route_update(GameUpdate { entity: monster, timestamp: now }, &state.lane_channels).await;
        }

        // 2. Spawn Coin (10% chance per tick)
        if !players.is_empty() && rand::rng().random_bool(0.1) {
            let x = rand::rng().random_range(0..MAP_WIDTH);
            let y = rand::rng().random_range(0..MAP_HEIGHT);
            let id = format!("coin-{}", rand::rng().random::<u32>());
            
            let update = GameUpdate {
                entity: Entity {
                    id: id.clone(),
                    x,
                    y,
                    kind: EntityKind::Coin,
                    color: "#FFD700".to_string(),
                    score: 0,
                    durability: 0,
                    spawn_time: now,
                    health: 0,
                },
                timestamp: now,
            };
            
            route_update(update, &state.lane_channels).await;
            tracing::info!("Spawned Coin {}", id);
        }

        // 3. Spawn Sword (Rarer, capped at 5 on map)
        // Base: 0.5%
        // Bonus: +0.1% per monster
        let sword_chance = 0.005 + (monsters.len() as f64 * 0.001);
        if !players.is_empty() && swords.len() < 5 && rand::rng().random_bool(sword_chance.clamp(0.005, 0.05)) {
            let x = rand::rng().random_range(0..MAP_WIDTH);
            let y = rand::rng().random_range(0..MAP_HEIGHT);
            let id = format!("sword-{}", rand::rng().random::<u32>());
            
            let update = GameUpdate {
                entity: Entity {
                    id: id.clone(),
                    x,
                    y,
                    kind: EntityKind::Sword,
                    color: "#00FFFF".to_string(),
                    score: 0,
                    durability: 5,
                    spawn_time: now,
                    health: 0,
                },
                timestamp: now,
            };
            route_update(update, &state.lane_channels).await;
            tracing::info!("Spawned Sword {}", id);
        }

        // 4. Spawn Monster (Increases with Player Score)
        let current_monster_count: usize = state.shards.iter()
            .map(|s| s.read().unwrap().entries.iter().filter(|(_k, (v, _, _))| v.kind == EntityKind::Monster).count())
            .sum();

        let total_score: u32 = players.iter().map(|p| p.score).sum();
        // Base: 2%
        // Scaling: +1% per ~200 score (0.00005 * 200 = 0.01)
        let spawn_chance = 0.02 + (total_score as f64 * 0.00005);

        if !players.is_empty() && current_monster_count < MAX_MONSTERS && rand::rng().random_bool(spawn_chance.clamp(0.02, 0.25)) {
             let x = rand::rng().random_range(0..MAP_WIDTH);
            let y = rand::rng().random_range(0..MAP_HEIGHT);
            let id = format!("mob-{}", rand::rng().random::<u32>());
            
            let update = GameUpdate {
                entity: Entity {
                    id: id.clone(),
                    x,
                    y,
                    kind: EntityKind::Monster,
                    color: "#FF0000".to_string(),
                    score: 0,
                    durability: 0,
                    spawn_time: now,
                    health: 0,
                },
                timestamp: now,
            };
            route_update(update, &state.lane_channels).await;
            tracing::info!("Spawned Monster {}", id);
        }
    }
}

async fn route_update(update: GameUpdate, lanes: &[mpsc::Sender<GameUpdate>]) {
    let mut hasher = seahash::SeaHasher::new();
    use std::hash::Hasher;
    hasher.write(update.entity.id.as_bytes());
    let hash = hasher.finish();
    let lane_idx = (hash as usize) % NUM_LANES;
    
    if let Some(lane) = lanes.get(lane_idx) {
        let _ = lane.send(update).await;
    }
}

// ============================================================================
// Handlers
// ============================================================================

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.tx.subscribe();
    let (mut sender, mut receiver) = socket.split();
    let player_id = Arc::new(std::sync::Mutex::new(None::<String>));

    // 1. Send Initial State
    // Iterate all shards and send all entities
    for shard in &state.shards {
        let entities: Vec<GameUpdate> = {
            let map = shard.read().unwrap();
            map.entries.iter()
                .map(|(_k, (v, ts, _node))| GameUpdate {
                    entity: v.clone(),
                    timestamp: *ts,
                })
                .collect()
        };
        
        for update in entities {
            if let Ok(msg) = serde_json::to_string(&update) {
                let _ = sender.send(Message::Text(msg)).await;
            }
        }
    }

    // Task to forward broadcast messages to this client
    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Task to receive messages from this client
    let lanes = state.lane_channels.clone();
    let pid_clone = player_id.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                match serde_json::from_str::<GameUpdate>(&text) {
                    Ok(mut update) => {
                        // Capture player ID
                        {
                            let mut pid = pid_clone.lock().unwrap();
                            if pid.is_none() && update.entity.kind == EntityKind::Player {
                                *pid = Some(update.entity.id.clone());
                            }
                        }

                        // Force server timestamp for consistency
                        update.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                        route_update(update, &lanes).await;
                    }
                    Err(e) => {
                        tracing::error!("Failed to parse client message: {} | Error: {}", text, e);
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };

    // Cleanup on disconnect
    let pid = player_id.lock().unwrap().clone();
    if let Some(id) = pid {
        tracing::info!("Player {} disconnected. Removing from world.", id);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let delete_update = GameUpdate {
            entity: Entity {
                id,
                x: 0, y: 0,
                kind: EntityKind::Deleted,
                color: "".to_string(),
                score: 0, durability: 0, spawn_time: 0, health: 0
            },
            timestamp: now
        };
        route_update(delete_update, &state.lane_channels).await;
    }
}
