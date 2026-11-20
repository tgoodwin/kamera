package interactive

import "runtime"

func OpenArgs(path string) []string {
	switch runtime.GOOS {
	case "darwin":
		return []string{"open", path}
	case "linux":
		return []string{"xdg-open", path}
	case "windows":
		return []string{"cmd", "/c", "start", "", path}
	default:
		return []string{"open", path}
	}
}
