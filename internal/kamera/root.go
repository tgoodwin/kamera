package kamera

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

type exitCodeError struct {
	code int
}

func (e *exitCodeError) Error() string {
	return fmt.Sprintf("exit code %d", e.code)
}

func wrapExitCode(code int) error {
	if code == 0 {
		return nil
	}
	return &exitCodeError{code: code}
}

func Run(args []string, stdout, stderr io.Writer) int {
	root := newRootCmd(stdout, stderr)
	root.SetArgs(args)

	if err := root.Execute(); err != nil {
		var codeErr *exitCodeError
		if errors.As(err, &codeErr) {
			return codeErr.code
		}
		fmt.Fprintln(stderr, err)
		return 1
	}

	return 0
}

func newRootCmd(stdout, stderr io.Writer) *cobra.Command {
	root := &cobra.Command{
		Use:           "kamera",
		Short:         "Kamera unified CLI",
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.SetOut(stdout)
	root.SetErr(stderr)

	root.AddCommand(newInspectCmd())
	root.AddCommand(newGenerateCmd())
	root.AddCommand(newDeterminizeCmd())
	root.AddCommand(newAnalyzeCmd())

	return root
}

func newInspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:                "inspect",
		Short:              "Inspect dependency graphs and exploration dumps",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			code, err := RunInspect(args)
			if err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), err)
			}
			return wrapExitCode(code)
		},
	}
}

func newGenerateCmd() *cobra.Command {
	return &cobra.Command{
		Use:                "generate",
		Short:              "Generate coverage inputs from hotspot analysis",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			code, err := RunGenerate(args)
			if err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), err)
			}
			return wrapExitCode(code)
		},
	}
}

func newDeterminizeCmd() *cobra.Command {
	return &cobra.Command{
		Use:                "determinize",
		Short:              "Rewrite Go code to use deterministic time",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return wrapExitCode(RunDeterminize(args, cmd.ErrOrStderr()))
		},
	}
}

func newAnalyzeCmd() *cobra.Command {
	return &cobra.Command{
		Use:                "analyze",
		Short:              "Run backward-trace analysis on exploration dumps",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return wrapExitCode(RunAnalyze(args, cmd.OutOrStdout(), cmd.ErrOrStderr()))
		},
	}
}
