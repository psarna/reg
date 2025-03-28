package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/psarna/reg/pkg/reg"
	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "reg",
		Short: "reg is a registry server",
	}

	var serveCmd = &cobra.Command{
		Use:   "serve",
		Short: "Start the registry server",
		Run:   runServe,
	}

	var bucket string
	serveCmd.Flags().StringVarP(&bucket, "bucket", "b", "", "Bucket name (required)")
	serveCmd.MarkFlagRequired("bucket")

	rootCmd.AddCommand(serveCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %v", err)
	}
}

func runServe(cmd *cobra.Command, args []string) {
	bucket, _ := cmd.Flags().GetString("bucket")

	ctx := context.Background()
	r, err := reg.NewRouter(ctx, bucket)
	if err != nil {
		log.Fatalf("Failed to create router: %v", err)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	port := ":2137"
	fmt.Printf("Server starting on %s with bucket '%s'...\n", port, bucket)
	log.Fatal(http.ListenAndServe(port, r))
}
