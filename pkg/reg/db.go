package reg

import (
	"database/sql"
	"fmt"
	"log/slog"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type RegistryDB struct {
	db *sql.DB
}

func initSQLite(path string) (*RegistryDB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	_, err = db.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}
	tables := []string{
		`CREATE TABLE IF NOT EXISTS repos (
			repository_id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE
		);`,
		`CREATE TABLE IF NOT EXISTS tags (
			tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
			repository_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			manifest_id INTEGER,
			UNIQUE(repository_id, name)
		);`,
		`CREATE TABLE IF NOT EXISTS manifests (
			manifest_id INTEGER PRIMARY KEY AUTOINCREMENT,
			repository_id INTEGER NOT NULL,
			tag_id INTEGER,
			manifest_json TEXT NOT NULL,
			UNIQUE(repository_id, tag_id)
		);`,
		`CREATE TABLE IF NOT EXISTS manifest_layers (
			manifest_id INTEGER NOT NULL,
			layer_id INTEGER NOT NULL,
			layer_index INTEGER NOT NULL,
			UNIQUE(manifest_id, layer_id, layer_index)
		);`,
		`CREATE TABLE IF NOT EXISTS layers (
			layer_id INTEGER PRIMARY KEY AUTOINCREMENT,
			digest TEXT NOT NULL UNIQUE,
			media_type TEXT NOT NULL,
			size INTEGER NOT NULL,
			UNIQUE(digest)
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
		JOIN repos ON repos.repository_id = manifests.repository_id
		JOIN tags ON tags.tag_id = manifests.tag_id
		WHERE repos.name = ? AND tags.name = ?`
	row := r.db.QueryRow(query, repo, tag)
	var manifestJSON string
	err := row.Scan(&manifestJSON)
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
	query := `INSERT INTO repos (name) VALUES (?) ON CONFLICT(name) DO NOTHING`
	_, err := r.db.Exec(query, repo)
	if err != nil {
		return fmt.Errorf("failed to register repo: %w", err)
	}

	query = `SELECT repository_id FROM repos WHERE name = ?`
	row := r.db.QueryRow(query, repo)
	var repoID int
	err = row.Scan(&repoID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("repo not found: %w", err)
		}
		return fmt.Errorf("failed to get repo id: %w", err)
	}

	query = `INSERT INTO tags (repository_id, name) VALUES (?, ?) ON CONFLICT(repository_id, name) DO NOTHING`
	_, err = r.db.Exec(query, repoID, tag)
	if err != nil {
		return fmt.Errorf("failed to register tag: %w", err)
	}

	query = `SELECT tag_id FROM tags WHERE repository_id = ? AND name = ?`
	row = r.db.QueryRow(query, repoID, tag)
	var tagID int
	err = row.Scan(&tagID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("tag not found: %w", err)
		}
		return fmt.Errorf("failed to get tag id: %w", err)
	}

	query = `INSERT INTO manifests (repository_id, tag_id, manifest_json) VALUES (?, ?, ?) ON CONFLICT(repository_id, tag_id) DO UPDATE SET manifest_json = ?`
	_, err = r.db.Exec(query, repoID, tagID, manifestBytes, manifestBytes)
	if err != nil {
		return fmt.Errorf("failed to store manifest: %w", err)
	}

	query = `SELECT manifest_id FROM manifests WHERE repository_id = ? AND tag_id = ?`
	row = r.db.QueryRow(query, repoID, tagID)
	var manifestID int
	err = row.Scan(&manifestID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("manifest not found: %w", err)
		}
		return fmt.Errorf("failed to get manifest id: %w", err)
	}

	query = `INSERT INTO layers (digest, media_type, size) VALUES (?, ?, ?) ON CONFLICT(digest) DO UPDATE SET media_type = ?, size = ?`
	for _, layer := range manifest.Layers {
		_, err = r.db.Exec(query, layer.Digest.String(), layer.MediaType, layer.Size, layer.MediaType, layer.Size)
		if err != nil {
			return fmt.Errorf("failed to store layer: %w", err)
		}
	}

	// Should only be effective if the manifest is updated
	purgeLayersQuery := `DELETE FROM manifest_layers WHERE manifest_id = ?`
	_, err = r.db.Exec(purgeLayersQuery, manifestID)
	if err != nil {
		return fmt.Errorf("failed to delete existing manifest layers: %w", err)
	}

	for i, layer := range manifest.Layers {
		query = `SELECT layer_id FROM layers WHERE digest = ?`
		row = r.db.QueryRow(query, layer.Digest.String())
		var layerID int
		err = row.Scan(&layerID)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("layer not found: %w", err)
			}
			return fmt.Errorf("failed to get layer id: %w", err)
		}
		_, err = r.db.Exec(
			`INSERT INTO manifest_layers (manifest_id, layer_id, layer_index) VALUES (?, ?, ?)`,
			manifestID,
			layerID,
			i,
		)
		if err != nil {
			return fmt.Errorf("failed to store manifest layer: %w", err)
		}
	}

	return nil
}

func (r *RegistryDB) ListTags(name string) ([]string, error) {
	query := `SELECT tags.name FROM tags
		JOIN repos ON repos.repository_id = tags.repository_id
		WHERE repos.name = ?`
	rows, err := r.db.Query(query, name)
	if err != nil {
		return nil, fmt.Errorf("failed to list tags: %w", err)
	}
	defer rows.Close()

	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("failed to scan tag: %w", err)
		}
		tags = append(tags, tag)
	}

	return tags, nil
}

func (r *RegistryDB) PutTags(repo string, tags []string) error {
	query := `INSERT INTO repos (name) VALUES (?) ON CONFLICT(name) DO NOTHING`
	_, err := r.db.Exec(query, repo)
	if err != nil {
		return fmt.Errorf("failed to register repo: %w", err)
	}

	query = `SELECT repository_id FROM repos WHERE name = ?`
	row := r.db.QueryRow(query, repo)
	var repoID int
	err = row.Scan(&repoID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("repo not found: %w", err)
		}
		return fmt.Errorf("failed to get repo id: %w", err)
	}

	for _, tag := range tags {
		query = `INSERT INTO tags (repository_id, name) VALUES (?, ?) ON CONFLICT(repository_id, name) DO NOTHING`
		_, err = r.db.Exec(query, repoID, tag)
		if err != nil {
			return fmt.Errorf("failed to register tag: %w", err)
		}
	}

	return nil
}
