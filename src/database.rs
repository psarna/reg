use anyhow::Result;
use rusqlite::Connection;
use serde_json::Value;
use std::sync::RwLock;

#[derive(Debug)]
pub struct Database {
    db_path: String,
    conn: RwLock<Connection>,
}

unsafe impl Sync for Database {}
unsafe impl Send for Database {}

impl Clone for Database {
    fn clone(&self) -> Self {
        let conn = match Connection::open(&self.db_path) {
            Ok(conn) => RwLock::new(conn),
            Err(_) => panic!("failed to open a new connection to the database"),
        };
        Database {
            db_path: self.db_path.clone(),
            conn,
        }
    }
}

impl Database {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = RwLock::new(Connection::open(db_path)?);
        Ok(Database {
            db_path: db_path.to_string(),
            conn,
        })
    }

    pub fn setup(&self) -> Result<()> {
        let conn = self.conn.read().unwrap();
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS repos (
            repository_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tags (
            tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
            repository_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            manifest_id, -- might be filled later if we got the tag info from list-tags only
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS manifests (
            manifest_id INTEGER PRIMARY KEY AUTOINCREMENT,
            repository_id INTEGER NOT NULL,
            tag_id INTEGER,
            digest TEXT NOT NULL,
            media_type TEXT NOT NULL,
            size INTEGER NOT NULL,
            config_id INTEGER,
            manifest_json TEXT NOT NULL,  -- Store the entire manifest as JSON string
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS manifest_layers (
            manifest_id INTEGER NOT NULL,
            layer_id INTEGER NOT NULL,
            layer_index INTEGER NOT NULL
        );",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS layers (
            layer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            digest TEXT NOT NULL,
            media_type TEXT NOT NULL,
            size INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS configs (
            config_id INTEGER PRIMARY KEY AUTOINCREMENT,
            digest TEXT NOT NULL,
            media_type TEXT NOT NULL,
            size INTEGER NOT NULL,
            config_json TEXT NOT NULL,  -- Store the entire config as JSON string
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );",
            [],
        )?;
        Ok(())
    }

    pub fn get_manifest(&self, repo: &str, tag: &str) -> Result<Option<String>> {
        let conn = self.conn.read().unwrap();
        let repository_id: i64 = conn.query_row(
            "SELECT repository_id FROM repos WHERE name = ?",
            [repo],
            |row| row.get(0),
        )?;
        let tag_id: i64 = conn.query_row(
            "SELECT tag_id FROM tags WHERE repository_id = ? AND name = ?",
            (&repository_id, tag),
            |row| row.get(0),
        )?;
        let manifest_json: String = conn.query_row(
            "SELECT manifest_json FROM manifests WHERE repository_id = ? AND tag_id = ?",
            (&repository_id, &tag_id),
            |row| row.get(0),
        )?;
        Ok(Some(manifest_json))
    }

    pub fn save_manifest(&self, repo: &str, tag: &str, manifest_json: &str) -> Result<()> {
        let conn = self.conn.read().unwrap();
        conn.execute("INSERT OR IGNORE INTO repos (name) VALUES (?)", [repo])?;
        let repository_id: i64 = conn.query_row(
            "SELECT repository_id FROM repos WHERE name = ?",
            [repo],
            |row| row.get(0),
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO tags (repository_id, name) VALUES (?, ?)",
            (&repository_id, tag),
        )?;
        let tag_id: i64 = conn.query_row(
            "SELECT tag_id FROM tags WHERE repository_id = ? AND name = ?",
            (&repository_id, tag),
            |row| row.get(0),
        )?;

        let json: Value = serde_json::from_str(manifest_json)?;

        conn.execute(
            "INSERT INTO manifests (repository_id, tag_id, digest, media_type, size, manifest_json) VALUES (?, ?, ?, ?, ?, ?)",
            (
                &repository_id,
                &tag_id,
                json.get("digest").unwrap_or(&Value::Null).as_str().unwrap_or(""),
                json.get("mediaType").unwrap_or(&Value::Null).as_str().unwrap_or(""),
                json.get("size").unwrap_or(&Value::Null).as_i64().unwrap_or(0),
                manifest_json,
            ),
        )?;
        // TODO: layers, configs, and all
        Ok(())
    }

    pub fn list_tags(&self, repo: &str) -> Result<Vec<String>> {
        let conn = self.conn.read().unwrap();
        let mut stmt = conn.prepare("SELECT name FROM tags WHERE repository_id = (SELECT repository_id FROM repos WHERE name = ?)")?;
        let tags_iter = stmt.query_map([repo], |row| row.get(0))?;

        let mut tags = Vec::new();
        for tag in tags_iter {
            tags.push(tag?);
        }
        Ok(tags)
    }

    pub fn save_tags(&self, repo: &str, tags: &[String]) -> Result<()> {
        let conn = self.conn.read().unwrap();
        conn.execute("INSERT OR IGNORE INTO repos (name) VALUES (?)", [repo])?;
        let repository_id: i64 = conn.query_row(
            "SELECT repository_id FROM repos WHERE name = ?",
            [repo],
            |row| row.get(0),
        )?;
        for tag in tags {
            conn.execute(
                "INSERT OR IGNORE INTO tags (repository_id, name) VALUES (?, ?)",
                (&repository_id, tag),
            )?;
        }
        Ok(())
    }
}
