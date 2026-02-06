// Command raidraccoon is the CLI entrypoint for the RaidRaccoon Deluxe service.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"raidraccoon/internal/config"
	"raidraccoon/internal/httpd"
	"raidraccoon/internal/rsync"
	"raidraccoon/internal/zfs"
)

func main() {
	if len(os.Args) < 2 {
		runServe(os.Args[1:])
		return
	}
	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "init":
		runInit(os.Args[2:])
	case "passwd":
		runPasswd(os.Args[2:])
	case "snapshot":
		runSnapshot(os.Args[2:])
	case "replicate":
		runReplicate(os.Args[2:])
	case "rsync":
		runRsync(os.Args[2:])
	default:
		runServe(os.Args[1:])
	}
}

const (
	defaultSystemConfigPath = "/usr/local/etc/raidraccoon.json"
	configEnvVar            = "RAIDRACCOON_CONFIG"
)

func defaultConfigPath(allowSystemCreate bool) string {
	if env := strings.TrimSpace(os.Getenv(configEnvVar)); env != "" {
		return env
	}
	if allowSystemCreate {
		return defaultSystemConfigPath
	}
	if config.Exists(defaultSystemConfigPath) {
		return defaultSystemConfigPath
	}
	return "raidraccoon.json"
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fs.String("config", defaultConfigPath(false), "config path")
	unsafeFlag := fs.Bool("unsafe", false, "disable command allowlist checks (dangerous)")
	_ = fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	cfg.ConfigPath = *configPath
	if *unsafeFlag {
		cfg.Unsafe = true
		fmt.Fprintln(os.Stderr, "WARNING: --unsafe disables command allowlist checks")
	}
	if cfg.BinaryPath == "" {
		if exe, err := os.Executable(); err == nil {
			cfg.BinaryPath = exe
		}
	}

	srv := httpd.New(cfg)
	addr := cfg.Server.ListenAddr
	if addr == "" {
		addr = "0.0.0.0:8080"
	}
	fmt.Printf("RaidRaccoon Deluxe listening on %s\n", addr)
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

func runInit(args []string) {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	configPath := fs.String("config", defaultConfigPath(true), "config path")
	_ = fs.Parse(args)

	if config.Exists(*configPath) {
		fmt.Printf("Config already exists at %s\n", *configPath)
		return
	}
	cfg, err := config.DefaultConfigWithPassword("changeme")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create config: %v\n", err)
		os.Exit(1)
	}
	if exe, err := os.Executable(); err == nil {
		cfg.BinaryPath = exe
	}
	if err := config.EnsureDir(*configPath); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create config dir: %v\n", err)
		os.Exit(1)
	}
	if err := config.Save(*configPath, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to save config: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Wrote config to %s (default password: changeme)\n", *configPath)
}

func runPasswd(args []string) {
	fs := flag.NewFlagSet("passwd", flag.ExitOnError)
	configPath := fs.String("config", defaultConfigPath(false), "config path")
	_ = fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("New password: ")
	pass1, _ := reader.ReadString('\n')
	fmt.Print("Confirm password: ")
	pass2, _ := reader.ReadString('\n')
	pass1 = strings.TrimSpace(pass1)
	pass2 = strings.TrimSpace(pass2)
	if pass1 == "" || pass1 != pass2 {
		fmt.Fprintln(os.Stderr, "passwords do not match")
		os.Exit(1)
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create salt: %v\n", err)
		os.Exit(1)
	}
	cfg.Auth.SaltHex = hex.EncodeToString(salt)
	cfg.Auth.PasswordHashHex = config.HashPasswordHex(cfg.Auth.SaltHex, pass1)
	if err := config.Save(*configPath, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to save config: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Password updated")
}

func runSnapshot(args []string) {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	configPath := fs.String("config", defaultConfigPath(false), "config path")
	dataset := fs.String("dataset", "", "dataset name")
	retention := fs.Int("retention", 7, "retention count")
	prefix := fs.String("prefix", "", "snapshot prefix")
	recursive := fs.Bool("recursive", false, "snapshot recursively")
	_ = fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	if *dataset == "" {
		fmt.Fprintln(os.Stderr, "--dataset is required")
		os.Exit(1)
	}
	if !zfs.ValidateDataset(cfg, *dataset) {
		fmt.Fprintln(os.Stderr, "invalid dataset name")
		os.Exit(1)
	}
	snapPrefix := *prefix
	if snapPrefix == "" {
		snapPrefix = cfg.ZFS.SnapshotPrefix
	}
	name := zfs.BuildSnapshotName(snapPrefix, time.Now())
	res, err := zfs.CreateSnapshot(context.Background(), cfg, *dataset, name, *recursive)
	if err != nil || res.ExitCode != 0 {
		fmt.Fprintf(os.Stderr, "snapshot failed: %s\n", res.Stderr)
		os.Exit(1)
	}
	_, err = zfs.EnforceRetention(context.Background(), cfg, *dataset, snapPrefix, *retention)
	if err != nil {
		fmt.Fprintf(os.Stderr, "retention cleanup failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Snapshot created: %s@%s\n", *dataset, name)
}

func runReplicate(args []string) {
	fs := flag.NewFlagSet("replicate", flag.ExitOnError)
	configPath := fs.String("config", defaultConfigPath(false), "config path")
	source := fs.String("source", "", "source dataset")
	target := fs.String("target", "", "target dataset")
	prefix := fs.String("prefix", "", "snapshot prefix")
	retention := fs.Int("retention", 0, "retention count")
	recursive := fs.Bool("recursive", false, "replicate recursively")
	force := fs.Bool("force", false, "force rollback on target")
	_ = fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	if *source == "" || *target == "" {
		fmt.Fprintln(os.Stderr, "--source and --target are required")
		os.Exit(1)
	}
	if !zfs.ValidDatasetName(*source) || !zfs.ValidateDataset(cfg, *source) {
		fmt.Fprintln(os.Stderr, "invalid source dataset")
		os.Exit(1)
	}
	if !zfs.ValidDatasetName(*target) || !zfs.ValidateDataset(cfg, *target) {
		fmt.Fprintln(os.Stderr, "invalid target dataset")
		os.Exit(1)
	}
	if *prefix != "" && !zfs.ValidSnapshotToken(*prefix) {
		fmt.Fprintln(os.Stderr, "invalid prefix")
		os.Exit(1)
	}
	res, err := zfs.ReplicateDataset(context.Background(), cfg, *source, *target, *prefix, *retention, *recursive, *force)
	if err != nil || res.ExitCode != 0 {
		fmt.Fprintf(os.Stderr, "replication failed: %s\n", res.Stderr)
		os.Exit(1)
	}
	fmt.Printf("Replication completed: %s -> %s\n", *source, *target)
}

func runRsync(args []string) {
	fs := flag.NewFlagSet("rsync", flag.ExitOnError)
	configPath := fs.String("config", defaultConfigPath(false), "config path")
	source := fs.String("source", "", "source path")
	target := fs.String("target", "", "target path")
	flagsRaw := fs.String("flags", "", "comma-separated rsync flags")
	_ = fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	if *source == "" || *target == "" {
		fmt.Fprintln(os.Stderr, "--source and --target are required")
		os.Exit(1)
	}
	flags := rsync.SplitFlags(*flagsRaw)
	res, err := rsync.Run(context.Background(), cfg, *source, *target, flags)
	if err != nil || res.ExitCode != 0 {
		fmt.Fprintf(os.Stderr, "rsync failed: %s\n", res.Stderr)
		os.Exit(1)
	}
	fmt.Printf("Rsync completed: %s -> %s\n", *source, *target)
}
