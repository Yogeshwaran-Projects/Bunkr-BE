package main

import (
	"fmt"
	"os"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	client := NewClient(getServerURL())

	switch os.Args[1] {
	case "upload":
		if len(os.Args) < 3 {
			fatal("usage: bunkr upload <file>")
		}
		cmdUpload(client, os.Args[2])

	case "get":
		if len(os.Args) < 3 {
			fatal("usage: bunkr get <name|id> --key <encryption-key> [-o output]")
		}
		output := ""
		key := ""
		for i := 3; i < len(os.Args); i++ {
			switch os.Args[i] {
			case "-o", "--output":
				if i+1 < len(os.Args) {
					output = os.Args[i+1]
					i++
				}
			case "--key", "-k":
				if i+1 < len(os.Args) {
					key = os.Args[i+1]
					i++
				}
			}
		}
		cmdGet(client, os.Args[2], key, output)

	case "ls", "list":
		cmdList(client)

	case "rm", "delete":
		if len(os.Args) < 3 {
			fatal("usage: bunkr rm <name|id>")
		}
		cmdDelete(client, os.Args[2])

	case "info":
		if len(os.Args) < 3 {
			fatal("usage: bunkr info <name|id>")
		}
		cmdInfo(client, os.Args[2])

	case "nodes":
		cmdNodes(client)

	case "health":
		cmdHealth(client)

	case "version":
		fmt.Printf("bunkr v%s\n", version)

	case "help", "--help", "-h":
		printUsage()

	default:
		fmt.Printf("unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`bunkr — Distributed File System CLI

Usage:
  bunkr upload <file>                     Upload a file to the cluster
  bunkr get <name|id> --key <key> [-o f]  Download a file
  bunkr ls                                List all files
  bunkr rm <name|id>                      Delete a file
  bunkr info <name|id>                    Show file details
  bunkr nodes                             Show cluster node status
  bunkr health                            Check cluster health
  bunkr version                           Show version

Environment:
  BUNKR_SERVER  Server URL (default: http://localhost:8080)`)
}

func getServerURL() string {
	url := os.Getenv("BUNKR_SERVER")
	if url == "" {
		return "http://localhost:8080"
	}
	return url
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
