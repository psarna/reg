use anyhow::Result;
use rusqlite::Connection;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct Database {
    db_path: String,
    conn: Arc<RwLock<Connection>>,
}

unsafe impl Sync for Database{}
unsafe impl Send for Database{}

impl Clone for Database {
    fn clone(&self) -> Self {
        // Try opening a new connection, use the existing one if it fails
        let conn = match Connection::open(&self.db_path) {
            Ok(conn) => Arc::new(RwLock::new(conn)),
            Err(_) => Arc::clone(&self.conn),
        };
        Database {
            db_path: self.db_path.clone(),
            conn: conn,
        }
    }
}

impl Database {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Arc::new(RwLock::new(Connection::open(db_path)?));
        Ok(Database {
            db_path: db_path.to_string(),
            conn,
        })
    }

    pub fn setup(&self) -> Result<()> {
        let conn = self.conn.write().unwrap();
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS repos (
            repository_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITHOUT ROWID;",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tags (
            tag_id INTEGER PRIMARY KEY,
            repository_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            manifest_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ); WITHOUT ROWID;",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS manifests (
            manifest_id INTEGER PRIMARY KEY,
            repository_id INTEGER NOT NULL,
            digest TEXT NOT NULL,
            media_type TEXT NOT NULL,
            size INTEGER NOT NULL,
            config_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ); WITHOUT ROWID;",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS manifest_layers (
            manifest_id INTEGER NOT NULL,
            layer_id INTEGER NOT NULL,
            layer_index INTEGER NOT NULL
        ); WITHOUT ROWID;",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS layers (
            layer_id INTEGER PRIMARY KEY,
            digest TEXT NOT NULL,
            media_type TEXT NOT NULL,
            size INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ); WITHOUT ROWID;",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS configs (
            config_id INTEGER PRIMARY KEY,
            digest TEXT NOT NULL,
            media_type TEXT NOT NULL,
            size INTEGER NOT NULL,
            config_json TEXT NOT NULL,  -- Store the entire config as JSON string
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ); WITHOUT ROWID;",
            [],
        )?;
        Ok(())
    }
}
