package halo

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
)

// InstallLine is the official way to install Halo. The SDK never downloads, updates or bundles a binary.
const InstallLine = "curl -fsSL https://memcode.ai/halo/install.sh | sh"

// ErrNotInstalled: no halo binary was found.
var ErrNotInstalled = errors.New("halo is not installed (install it with: " + InstallLine + ")")

// Find locates the installed halo binary: Options.BinPath, then $HALO_BIN, then `halo` on PATH, then the
// installer's locations ($HALO_INSTALL_DIR, /usr/local/bin, ~/.local/bin).
func Find(binPath string) (string, error) {
	if binPath != "" {
		return checked(binPath)
	}
	if p := os.Getenv("HALO_BIN"); p != "" {
		return checked(p)
	}
	if p, err := exec.LookPath("halo"); err == nil {
		return p, nil
	}
	var dirs []string
	if d := os.Getenv("HALO_INSTALL_DIR"); d != "" {
		dirs = append(dirs, d)
	}
	dirs = append(dirs, "/usr/local/bin")
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".local", "bin"))
	}
	for _, d := range dirs {
		if p, err := checked(filepath.Join(d, "halo")); err == nil {
			return p, nil
		}
	}
	return "", ErrNotInstalled
}

func checked(p string) (string, error) {
	fi, err := os.Stat(p)
	if err != nil || fi.IsDir() || fi.Mode()&0o111 == 0 {
		return "", ErrNotInstalled
	}
	return p, nil
}
