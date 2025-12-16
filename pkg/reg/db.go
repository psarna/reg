package reg

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type RegistryDB struct {
	db *sqlx.DB
}

func initSQLite(path string) (*RegistryDB, error) {
	db, err := sqlx.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	_, err = db.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}

	tables := []string{
		`CREATE TABLE IF NOT EXISTS tags (
			repository TEXT NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY(repository, name)
		);`,
		`CREATE TABLE IF NOT EXISTS manifests (
			tag_rowid INTEGER NOT NULL,
			manifest_json TEXT NOT NULL,
			PRIMARY KEY(tag_rowid)
		);`,
		`CREATE TABLE IF NOT EXISTS manifest_layers (
			manifest_rowid INTEGER NOT NULL,
			layer_digest TEXT NOT NULL,
			layer_index INTEGER NOT NULL,
			PRIMARY KEY(manifest_rowid, layer_digest, layer_index)
		);`,
		`CREATE TABLE IF NOT EXISTS layers (
			digest TEXT PRIMARY KEY,
			media_type TEXT NOT NULL,
			size INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS upload_sessions (
			upload_id TEXT PRIMARY KEY,
			repository TEXT NOT NULL,
			digest TEXT,
			s3_upload_id TEXT,
			s3_key TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			last_activity DATETIME DEFAULT CURRENT_TIMESTAMP,
			total_size INTEGER,
			uploaded_size INTEGER DEFAULT 0
		);`,
	}

	for _, table := range tables {
		slog.Debug("Creating table", "table", table)
		_, err = db.Exec(table)
		if err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return &RegistryDB{db: db}, nil
}

func (r *RegistryDB) GetManifest(repo string, tag string) (string, error) {
	query := `SELECT manifest_json FROM manifests 
		JOIN tags ON tags.rowid = manifests.tag_rowid
		WHERE tags.repository = ? AND tags.name = ?`

	var manifestJSON string
	err := r.db.Get(&manifestJSON, query, repo, tag)

	slog.Debug("Retrieved manifest", "repo", repo, "tag", tag)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("manifest not found for repository %s and tag %s", repo, tag)
		}
		return "", fmt.Errorf("failed to get manifest: %w", err)
	}

	return manifestJSON, nil
}

func (r *RegistryDB) PutManifest(repo string, tag string, manifestBytes string, manifest *v1.Manifest) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	query := `INSERT INTO tags (repository, name) VALUES (?, ?) ON CONFLICT(repository, name) DO NOTHING`
	_, err = tx.Exec(query, repo, tag)
	if err != nil {
		return fmt.Errorf("failed to register tag: %w", err)
	}

	var tagRowID int64
	query = `SELECT rowid FROM tags WHERE repository = ? AND name = ?`
	err = tx.Get(&tagRowID, query, repo, tag)
	if err != nil {
		return fmt.Errorf("failed to get tag rowid: %w", err)
	}

	query = `INSERT INTO manifests (tag_rowid, manifest_json) VALUES (?, ?) 
		ON CONFLICT(tag_rowid) DO UPDATE SET manifest_json = ?`
	_, err = tx.Exec(query, tagRowID, manifestBytes, manifestBytes)
	if err != nil {
		return fmt.Errorf("failed to store manifest: %w", err)
	}

	query = `INSERT INTO layers (digest, media_type, size) VALUES (?, ?, ?) 
		ON CONFLICT(digest) DO UPDATE SET media_type = ?, size = ?`
	for _, layer := range manifest.Layers {
		_, err = tx.Exec(query, layer.Digest.String(), layer.MediaType, layer.Size, layer.MediaType, layer.Size)
		if err != nil {
			return fmt.Errorf("failed to store layer: %w", err)
		}
	}

	purgeLayersQuery := `DELETE FROM manifest_layers WHERE manifest_rowid = (SELECT rowid FROM manifests WHERE tag_rowid = ?)`
	_, err = tx.Exec(purgeLayersQuery, tagRowID)
	if err != nil {
		return fmt.Errorf("failed to delete existing manifest layers: %w", err)
	}

	var manifestRowID int64
	query = `SELECT rowid FROM manifests WHERE tag_rowid = ?`
	err = tx.Get(&manifestRowID, query, tagRowID)
	if err != nil {
		return fmt.Errorf("failed to get manifest rowid: %w", err)
	}

	for i, layer := range manifest.Layers {
		_, err = tx.Exec(
			`INSERT INTO manifest_layers (manifest_rowid, layer_digest, layer_index) VALUES (?, ?, ?)`,
			manifestRowID,
			layer.Digest.String(),
			i,
		)
		if err != nil {
			return fmt.Errorf("failed to store manifest layer: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (r *RegistryDB) ListTags(repo string) ([]string, error) {
	var tags []string
	query := `SELECT name FROM tags WHERE repository = ?`

	err := r.db.Select(&tags, query, repo)
	if err != nil {
		return nil, fmt.Errorf("failed to list tags: %w", err)
	}

	return tags, nil
}

func (r *RegistryDB) PutTags(repo string, tags []string) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	query := `INSERT INTO tags (repository, name) VALUES (?, ?) ON CONFLICT(repository, name) DO NOTHING`
	for _, tag := range tags {
		_, err := tx.Exec(query, repo, tag)
		if err != nil {
			return fmt.Errorf("failed to register tag: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (r *RegistryDB) ListRepositories(continuationToken *string, n int) ([]string, *string, error) {
	if continuationToken == nil {
		token := ""
		continuationToken = &token
	}
	query := `SELECT repository FROM tags WHERE repository > ? LIMIT ?`
	var repos []string
	err := r.db.Select(&repos, query, continuationToken, n)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list repositories: %w", err)
	}

	if len(repos) == 0 {
		return nil, nil, nil
	}

	return repos, &repos[len(repos)-1], nil
}

func (r *RegistryDB) Exists(repo string, tag string) bool {
	query := `SELECT 1 FROM tags WHERE repository = ? AND name = ?`
	var dummy int
	return r.db.Get(&dummy, query, repo, tag) == nil
}

func (r *RegistryDB) CreateUploadSession(uploadID, repository, s3Key string) error {
	query := `INSERT INTO upload_sessions (upload_id, repository, s3_key) VALUES (?, ?, ?)`
	_, err := r.db.Exec(query, uploadID, repository, s3Key)
	if err != nil {
		return fmt.Errorf("failed to create upload session: %w", err)
	}
	return nil
}

func (r *RegistryDB) UpdateUploadSession(uploadID, s3UploadID string, uploadedSize int64) error {
	query := `UPDATE upload_sessions SET s3_upload_id = ?, uploaded_size = ?, last_activity = CURRENT_TIMESTAMP WHERE upload_id = ?`
	_, err := r.db.Exec(query, s3UploadID, uploadedSize, uploadID)
	if err != nil {
		return fmt.Errorf("failed to update upload session: %w", err)
	}
	return nil
}

func (r *RegistryDB) GetUploadSession(uploadID string) (string, string, int64, error) {
	query := `SELECT COALESCE(s3_upload_id, ''), COALESCE(s3_key, ''), uploaded_size FROM upload_sessions WHERE upload_id = ?`
	var s3UploadID, s3Key string
	var uploadedSize int64
	err := r.db.QueryRow(query, uploadID).Scan(&s3UploadID, &s3Key, &uploadedSize)
	if err != nil {
		return "", "", 0, fmt.Errorf("failed to get upload session: %w", err)
	}
	return s3UploadID, s3Key, uploadedSize, nil
}

func (r *RegistryDB) DeleteUploadSession(uploadID string) error {
	query := `DELETE FROM upload_sessions WHERE upload_id = ?`
	_, err := r.db.Exec(query, uploadID)
	if err != nil {
		return fmt.Errorf("failed to delete upload session: %w", err)
	}
	return nil
}

func (r *RegistryDB) GetStaleUploadSessions(maxAge string) ([]string, error) {
	query := `SELECT upload_id FROM upload_sessions WHERE last_activity < datetime('now', ?)`
	var uploadIDs []string
	err := r.db.Select(&uploadIDs, query, maxAge)
	if err != nil {
		return nil, fmt.Errorf("failed to get stale upload sessions: %w", err)
	}
	return uploadIDs, nil
}

func (r *RegistryDB) Close() error {
	if err := r.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	return nil
}
