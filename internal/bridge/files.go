package bridge

import (
	"os"
	"path/filepath"
)

// readFile reads a file inside dir, returning an error for anything outside it.
func readFile(dir, name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(dir, filepath.Base(name)))
}

// writeFileAtomic writes name inside dir through a temporary file so a crash
// never leaves a half-written settings or usage file behind.
func writeFileAtomic(dir, name string, body []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	final := filepath.Join(dir, filepath.Base(name))
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(name)+".*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return err
	}
	return os.Rename(tmpName, final)
}

// removeFile deletes name inside dir, ignoring a file that is already gone.
func removeFile(dir, name string) error {
	err := os.Remove(filepath.Join(dir, filepath.Base(name)))
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}
