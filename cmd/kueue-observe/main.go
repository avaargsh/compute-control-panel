// kueue-observe performs a one-shot, read-only observation of a known,
// UID-pinned Kubernetes Job from inside a Pod with mounted ServiceAccount files.
// It never creates, patches, deletes or schedules Kubernetes resources.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/avaargsh/compute-control-panel/internal/provider"
)

const serviceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount"

type observationOptions struct {
	apiURL    string
	caPath    string
	tokenPath string
	query     provider.KubernetesJobQuery
}

func parseOptions(args []string, getenv func(string) string) (observationOptions, error) {
	var opts observationOptions
	flags := flag.NewFlagSet("kueue-observe", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.query.TenantID, "tenant", "", "expected CCP tenant ID")
	flags.StringVar(&opts.query.WorkloadID, "workload", "", "expected CCP Workload ID")
	flags.Uint64Var(&opts.query.DesiredGeneration, "generation", 0, "expected desired generation")
	flags.StringVar(&opts.query.Namespace, "namespace", "", "authorized Kubernetes namespace")
	flags.StringVar(&opts.query.JobName, "job", "", "existing Kubernetes Job name")
	flags.StringVar(&opts.query.ExpectedJobUID, "uid", "", "immutable expected Kubernetes Job UID")
	flags.StringVar(&opts.caPath, "ca", serviceAccountPath+"/ca.crt", "mounted Kubernetes API CA path")
	flags.StringVar(&opts.tokenPath, "token", serviceAccountPath+"/token", "mounted ServiceAccount token path")
	if err := flags.Parse(args); err != nil {
		return observationOptions{}, err
	}
	if len(flags.Args()) != 0 || getenv == nil {
		return observationOptions{}, errors.New("unexpected arguments or absent environment")
	}
	for _, s := range []string{
		opts.query.TenantID, opts.query.WorkloadID, opts.query.Namespace,
		opts.query.JobName, opts.query.ExpectedJobUID, opts.caPath, opts.tokenPath,
	} {
		if s == "" || s != strings.TrimSpace(s) {
			return observationOptions{}, errors.New("all Job identity and credential-path flags are required")
		}
	}
	if opts.query.DesiredGeneration == 0 {
		return observationOptions{}, errors.New("desired generation must be positive")
	}
	host := getenv("KUBERNETES_SERVICE_HOST")
	port := getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 || host == "" ||
		host != strings.TrimSpace(host) || strings.ContainsAny(host, "/?@#\\") {
		return observationOptions{}, errors.New("invalid Kubernetes API service host or HTTPS port")
	}
	opts.apiURL = "https://" + net.JoinHostPort(host, port)
	return opts, nil
}

func run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	opts, err := parseOptions(args, getenv)
	if err != nil {
		return err
	}
	reader, err := provider.NewServiceAccountKubernetesObservationReader(
		opts.apiURL, opts.caPath, opts.tokenPath,
	)
	if err != nil {
		return err
	}
	observeCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	collected, err := reader.Collect(observeCtx, opts.query)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(collected)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "kueue-observe: read-only observation failed:", err)
		os.Exit(1)
	}
}
