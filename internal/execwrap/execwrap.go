// Package execwrap executes privileged commands via sudo -n with output limits.
package execwrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"raidraccoon/internal/config"
)

type Result struct {
	Stdout    string
	Stderr    string
	ExitCode  int
	Truncated bool
}

// Run executes absCmd via `sudo -n` and returns captured output.
// This is the only place in the codebase that shells out for privileged actions.
func Run(ctx context.Context, absCmd string, args []string, stdin []byte, limits config.Limits) (Result, error) {
	if absCmd == "" || absCmd[0] != '/' {
		return Result{}, fmt.Errorf("command must be absolute")
	}
	if limits.MaxRuntimeSeconds <= 0 {
		limits.MaxRuntimeSeconds = 120
	}
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(limits.MaxRuntimeSeconds)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(execCtx, "sudo", append([]string{"-n", absCmd}, args...)...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return Result{}, err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return Result{}, err
	}

	if err := cmd.Start(); err != nil {
		return Result{}, err
	}

	outBytes, outTrunc, err := config.ReadAllLimited(stdoutPipe, limits.MaxOutputBytes)
	if err != nil {
		return Result{}, err
	}
	errBytes, errTrunc, err := config.ReadAllLimited(stderrPipe, limits.MaxOutputBytes)
	if err != nil {
		return Result{}, err
	}

	err = cmd.Wait()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else if errors.Is(err, context.DeadlineExceeded) {
			exitCode = 124
		} else {
			exitCode = 1
		}
	}

	return Result{
		Stdout:    string(outBytes),
		Stderr:    string(errBytes),
		ExitCode:  exitCode,
		Truncated: outTrunc || errTrunc,
	}, nil
}
