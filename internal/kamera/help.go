package kamera

func isHelpArg(arg string) bool {
	return arg == "-h" || arg == "--help" || arg == "help"
}
