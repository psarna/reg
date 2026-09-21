package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

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
	var bootstrap bool
	var bootstrapTagsOnly bool
	var uiAddress string
	serveCmd.Flags().StringVarP(&bucket, "bucket", "b", "", "Bucket name (required)")
	serveCmd.Flags().BoolVarP(&bootstrap, "bootstrap", "B", false, "Bootstrap the registry from S3 (might take a few centuries for large registries)")
	serveCmd.Flags().BoolVar(&bootstrapTagsOnly, "bootstrap-tags-only", false, "Bootstrap repository and tag metadata without fetching manifests")
	serveCmd.Flags().StringVar(&uiAddress, "ui", "", "Start the web dashboard on this address, for example localhost:8080")
	serveCmd.MarkFlagRequired("bucket")

	rootCmd.AddCommand(serveCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %v", err)
	}
}

const splash = `
 $$$$$$\   $$$$$$\   $$$$$$\  
$$  __$$\ $$  __$$\ $$  __$$\ 
$$ |  \__|$$$$$$$$ |$$ /  $$ |
$$ |      $$   ____|$$ |  $$ |
$$ |      \$$$$$$$\ \$$$$$$$ |
\__|       \_______| \____$$ |
                    $$\   $$ |
                    \$$$$$$  |
                     \______/ `

func runServe(cmd *cobra.Command, args []string) {
	bucket, err := cmd.Flags().GetString("bucket")
	if err != nil {
		log.Fatalf("Failed to get bucket flag: %v", err)
	}
	bootstrap, err := cmd.Flags().GetBool("bootstrap")
	if err != nil {
		slog.Error("Failed to get bootstrap flag", "err", err)
	}
	bootstrapTagsOnly, err := cmd.Flags().GetBool("bootstrap-tags-only")
	if err != nil {
		slog.Error("Failed to get bootstrap-tags-only flag", "err", err)
	}
	uiAddress, err := cmd.Flags().GetString("ui")
	if err != nil {
		slog.Error("Failed to get ui flag", "err", err)
	}

	ctx := context.Background()
	registry, err := reg.NewRegistry(ctx, bucket)
	if err != nil {
		log.Fatalf("Failed to create registry: %v", err)
	}
	defer registry.Close()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT)
	go func() {
		sig := <-signalChan
		fmt.Printf("Received signal: %v, running cleanup\n", sig)
		registry.Close()
		os.Exit(0)
	}()

	if bootstrap || bootstrapTagsOnly {
		bootstrapFn := registry.Bootstrap
		if bootstrapTagsOnly {
			bootstrapFn = registry.BootstrapTagsOnly
		}
		if err := bootstrapFn(ctx); err != nil {
			slog.Error("Failed to bootstrap registry", "err", err)
			return
		}
		slog.Info("Bootstrap completed")
	}

	if uiAddress != "" {
		ui := reg.NewUIHandler(registry)
		go func() {
			slog.Info("UI starting", "address", uiAddress)
			if err := http.ListenAndServe(uiAddress, ui); err != nil {
				slog.Error("UI stopped", "error", err)
			}
		}()
	}

	r, err := reg.NewRouter(ctx, registry)
	if err != nil {
		log.Fatalf("Failed to create router: %v", err)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	port := ":2137"
	fmt.Println(splash)
	fmt.Println()
	fmt.Printf("Server starting on %s with bucket '%s'...\n", port, bucket)
	log.Fatal(http.ListenAndServe(port, r))
}
