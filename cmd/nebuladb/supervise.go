package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

// superviseEnv marks a child started by the supervisor.
const superviseEnv = "NEBULA_SUPERVISED"

// supervise runs the server as a child process and restarts it when it is
// killed by a signal (the Inspector's "crash it" button sends SIGKILL to
// demonstrate recovery). A clean exit, or an error exit such as a bad flag,
// ends supervision so mistakes do not become restart loops.
func supervise(args []string) error {
	var childArgs []string
	for _, a := range args {
		if a == "-supervise" || a == "--supervise" || a == "-supervise=true" {
			continue
		}
		childArgs = append(childArgs, a)
	}
	childArgs = append(childArgs, "-dev") // the crash endpoint needs dev mode

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	for generation := 1; ; generation++ {
		cmd := exec.Command(os.Args[0], childArgs...)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		cmd.Env = append(os.Environ(), superviseEnv+"=1")
		if err := cmd.Start(); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "supervisor: started server pid %d (generation %d)\n", cmd.Process.Pid, generation)

		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		stopping := false
	wait:
		for {
			select {
			case s := <-sig:
				stopping = true
				cmd.Process.Signal(s) // let the server shut down gracefully
			case err := <-done:
				if stopping {
					return nil
				}
				if ee, ok := err.(*exec.ExitError); ok {
					if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
						fmt.Fprintf(os.Stderr, "supervisor: server killed by %v; restarting in 300ms\n", ws.Signal())
						time.Sleep(300 * time.Millisecond)
						break wait
					}
				}
				if err != nil {
					return fmt.Errorf("server exited with error: %w (not restarting)", err)
				}
				return nil
			}
		}
	}
}
