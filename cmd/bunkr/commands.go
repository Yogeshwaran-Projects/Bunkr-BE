package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
)

func cmdUpload(client *Client, filePath string) {
	info, err := os.Stat(filePath)
	if err != nil {
		fatal(fmt.Sprintf("file not found: %s", filePath))
	}

	fileName := filepath.Base(filePath)
	fileSize := info.Size()

	fmt.Printf("\n  %sUploading%s %s (%s)\n", colorBold, colorReset, fileName, humanSize(fileSize))

	result, err := client.Upload(filePath)
	if err != nil {
		fatal(fmt.Sprintf("  %s✗%s Upload failed: %v", colorRed, colorReset, err))
	}

	if !result.Ok {
		fatal(fmt.Sprintf("  %s✗%s %s", colorRed, colorReset, result.Error))
	}

	fmt.Printf("  %s✓%s Stored successfully\n\n", colorGreen, colorReset)
	fmt.Printf("  File ID:    %s\n", result.FileID)
	fmt.Printf("  Chunks:     %d (4 MB each)\n", result.Chunks)
	fmt.Printf("  Replicas:   %dx across cluster\n", result.Replicas)
	fmt.Printf("  Encryption: AES-256-GCM\n")
	fmt.Printf("\n  %sEncryption Key (save this!):%s\n", colorYellow, colorReset)
	fmt.Printf("  %s\n\n", result.EncryptionKey)
}

func cmdGet(client *Client, nameOrID, encKey, output string) {
	if encKey == "" {
		fatal("encryption key required: bunkr get <name> --key <key>")
	}

	fmt.Printf("\n  %sDownloading%s %s...\n", colorBold, colorReset, nameOrID)

	data, filename, err := client.Download(nameOrID, encKey)
	if err != nil {
		fatal(fmt.Sprintf("  %s✗%s %v", colorRed, colorReset, err))
	}

	outPath := output
	if outPath == "" {
		outPath = filename
	}

	if err := os.WriteFile(outPath, data, 0644); err != nil {
		fatal(fmt.Sprintf("  %s✗%s Failed to write: %v", colorRed, colorReset, err))
	}

	fmt.Printf("  %s✓%s Saved to %s (%s)\n\n", colorGreen, colorReset, outPath, humanSize(int64(len(data))))
}

func cmdList(client *Client) {
	result, err := client.ListFiles()
	if err != nil {
		fatal(fmt.Sprintf("failed to list files: %v", err))
	}

	if !result.Ok {
		fatal(result.Error)
	}

	if len(result.Files) == 0 {
		fmt.Print("\n  No files stored yet.\n\n")
		return
	}

	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  %sNAME\tSIZE\tCHUNKS\tID%s\n", colorDim, colorReset)
	fmt.Fprintf(w, "  %s────\t────\t──────\t──%s\n", colorDim, colorReset)

	for _, f := range result.Files {
		id := f.ID
		if len(id) > 12 {
			id = id[:12]
		}
		fmt.Fprintf(w, "  %s\t%s\t%d\t%s\n", f.Name, humanSize(f.Size), len(f.Chunks), id)
	}
	w.Flush()

	fmt.Printf("\n  %s%d file(s)%s\n\n", colorDim, result.Count, colorReset)
}

func cmdDelete(client *Client, nameOrID string) {
	fmt.Printf("\n  Deleting %s...\n", nameOrID)

	result, err := client.DeleteFile(nameOrID)
	if err != nil {
		fatal(fmt.Sprintf("  %s✗%s %v", colorRed, colorReset, err))
	}

	if !result.Ok {
		fatal(fmt.Sprintf("  %s✗%s %s", colorRed, colorReset, result.Error))
	}

	fmt.Printf("  %s✓%s Deleted\n\n", colorGreen, colorReset)
}

func cmdInfo(client *Client, nameOrID string) {
	result, err := client.GetFileInfo(nameOrID)
	if err != nil {
		fatal(fmt.Sprintf("failed: %v", err))
	}

	if !result.Ok {
		fatal(result.Error)
	}

	f := result.File

	fmt.Printf("\n  %sFile: %s%s\n", colorBold, f.Name, colorReset)
	fmt.Printf("  ID:         %s\n", f.ID)
	fmt.Printf("  Size:       %s\n", humanSize(f.Size))
	fmt.Printf("  Chunks:     %d\n", len(f.Chunks))
	fmt.Printf("  Encryption: AES-256-GCM\n")

	if len(f.Chunks) > 0 {
		fmt.Printf("\n  %sCHUNK\tSIZE\tNODES%s\n", colorDim, colorReset)
		fmt.Printf("  %s─────\t────\t─────%s\n", colorDim, colorReset)

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		for _, c := range f.Chunks {
			chunkID := c.ID
			if len(chunkID) > 8 {
				chunkID = chunkID[:8]
			}
			fmt.Fprintf(w, "  #%d %s\t%s\t%s\n", c.Index, chunkID, humanSize(c.Size), strings.Join(c.Nodes, ", "))
		}
		w.Flush()
	}
	fmt.Println()
}

func cmdNodes(client *Client) {
	nodes, err := client.Nodes()
	if err != nil {
		fatal(fmt.Sprintf("failed: %v", err))
	}

	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  %sNODE\tSTATE\tTERM\tCHUNKS\tDISK\tSTATUS%s\n", colorDim, colorReset)
	fmt.Fprintf(w, "  %s────\t─────\t────\t──────\t────\t──────%s\n", colorDim, colorReset)

	for _, n := range nodes {
		status := fmt.Sprintf("%s● healthy%s", colorGreen, colorReset)
		state := n.State
		if !n.Alive {
			status = fmt.Sprintf("%s● down%s", colorRed, colorReset)
			state = "Dead"
		}

		if n.IsLeader {
			state = fmt.Sprintf("%s%s (leader)%s", colorCyan, n.State, colorReset)
		}

		fmt.Fprintf(w, "  node-%d\t%s\t%d\t%d\t%s\t%s\n",
			n.ID, state, n.Term, n.ChunkCount, humanSize(n.DiskUsage), status)
	}
	w.Flush()
	fmt.Println()
}

func cmdHealth(client *Client) {
	result, err := client.Health()
	if err != nil {
		fatal(fmt.Sprintf("  %s✗%s Cannot reach server: %v", colorRed, colorReset, err))
	}

	fmt.Println()
	if result.Status == "healthy" {
		fmt.Printf("  %s✓ Cluster healthy%s\n", colorGreen, colorReset)
	} else {
		fmt.Printf("  %s✗ %s%s\n", colorRed, result.Status, colorReset)
	}
	fmt.Printf("  Leader:     node-%d\n", result.Leader)
	fmt.Printf("  Files:      %d\n", result.FileCount)
	fmt.Println()
}

func humanSize(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	}
	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes)/(1024*1024))
	}
	return fmt.Sprintf("%.1f GB", float64(bytes)/(1024*1024*1024))
}
