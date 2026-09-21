package reg

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUIShowsSQLiteData(t *testing.T) {
	db, err := initSQLite("file:ui-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.db.Exec(`
		INSERT INTO tags(repository, name) VALUES ('team/app', 'latest');
		INSERT INTO manifests(tag_rowid, manifest_json) VALUES (1, '{"schemaVersion":2}');
		INSERT INTO layers(digest, media_type, size) VALUES ('sha256:abc', 'application/vnd.oci.image.layer.v1.tar', 2048);`); err != nil {
		t.Fatal(err)
	}

	handler := NewUIHandler(&Registry{db: db})
	for _, test := range []struct {
		path string
		want string
	}{
		{path: "/", want: "team/app"},
		{path: "/repository?name=team%2Fapp", want: "latest"},
		{path: "/manifest?repository=team%2Fapp&tag=latest", want: "schemaVersion"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest("GET", test.path, nil))
		if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), test.want) {
			t.Fatalf("GET %s: status %d, body missing %q", test.path, recorder.Code, test.want)
		}
	}
}
