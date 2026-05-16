package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/distributed-systems/internal/config"
	"github.com/distributed-systems/internal/logger"
	calculatorpb "github.com/distributed-systems/internal/rpc/proto"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

func main() {
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	log, _ := logger.New(cfg.Log.Level, "console")
	defer log.Sync()

	conn, err := dialWithRetry(cfg, log)
	if err != nil {
		log.Fatal("could not connect to gRPC server", zap.Error(err))
	}
	defer conn.Close()

	client := calculatorpb.NewCalculatorServiceClient(conn)

	// Health check on startup
	hCtx, hCancel := context.WithTimeout(context.Background(), 5*time.Second)
	health, err := client.HealthCheck(hCtx, &calculatorpb.HealthRequest{Service: "calculator"})
	hCancel()
	if err != nil {
		log.Fatal("health check failed", zap.Error(err))
	}
	log.Info("server healthy", zap.String("status", health.GetStatus()), zap.String("version", health.GetVersion()))

	fmt.Println("\n=== Distributed Calculator Client ===")
	fmt.Println("Binary ops:  add sub mul div pow  <a> <b>")
	fmt.Println("Unary ops:   fact fib             <n>")
	fmt.Println("Type 'quit' to exit\n")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		op := strings.ToLower(parts[0])
		if op == "quit" || op == "exit" {
			fmt.Println("Goodbye!")
			break
		}
		if err := runOperation(client, op, parts[1:]); err != nil {
			fmt.Printf("  ✗ Error: %v\n", err)
		}
	}
}

func runOperation(client calculatorpb.CalculatorServiceClient, op string, args []string) error {
	reqID := uuid.New().String()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", reqID)

	start := time.Now()
	var resp *calculatorpb.CalculationResponse
	var err error

	switch op {
	case "add":
		a, b, e := parseTwoArgs(args)
		if e != nil {
			return e
		}
		resp, err = client.Add(ctx, &calculatorpb.BinaryRequest{RequestId: reqID, OperandA: a, OperandB: b})
	case "sub", "subtract":
		a, b, e := parseTwoArgs(args)
		if e != nil {
			return e
		}
		resp, err = client.Subtract(ctx, &calculatorpb.BinaryRequest{RequestId: reqID, OperandA: a, OperandB: b})
	case "mul", "multiply":
		a, b, e := parseTwoArgs(args)
		if e != nil {
			return e
		}
		resp, err = client.Multiply(ctx, &calculatorpb.BinaryRequest{RequestId: reqID, OperandA: a, OperandB: b})
	case "div", "divide":
		a, b, e := parseTwoArgs(args)
		if e != nil {
			return e
		}
		resp, err = client.Divide(ctx, &calculatorpb.BinaryRequest{RequestId: reqID, OperandA: a, OperandB: b})
	case "pow", "power":
		a, b, e := parseTwoArgs(args)
		if e != nil {
			return e
		}
		resp, err = client.Power(ctx, &calculatorpb.BinaryRequest{RequestId: reqID, OperandA: a, OperandB: b})
	case "fact", "factorial":
		n, e := parseOneArg(args)
		if e != nil {
			return e
		}
		resp, err = client.Factorial(ctx, &calculatorpb.UnaryRequest{RequestId: reqID, N: int64(n)})
	case "fib", "fibonacci":
		n, e := parseOneArg(args)
		if e != nil {
			return e
		}
		resp, err = client.Fibonacci(ctx, &calculatorpb.UnaryRequest{RequestId: reqID, N: int64(n)})
	default:
		return fmt.Errorf("unknown operation %q — try: add sub mul div pow fact fib", op)
	}

	rtt := time.Since(start)
	if err != nil {
		return fmt.Errorf("gRPC error: %w", err)
	}
	printResult(resp, rtt)
	return nil
}

func printResult(resp *calculatorpb.CalculationResponse, rtt time.Duration) {
	ops := make([]string, len(resp.GetOperands()))
	for i, o := range resp.GetOperands() {
		ops[i] = strconv.FormatFloat(o, 'f', -1, 64)
	}
	if resp.GetStatus() == "error" {
		fmt.Printf("  ✗ [%s](%s) → Error: %s\n", resp.GetOperation(), strings.Join(ops, ", "), resp.GetErrorMessage())
		return
	}
	fmt.Printf("  ✓ [%s](%s) = %.10g\n", resp.GetOperation(), strings.Join(ops, ", "), resp.GetResult())
	fmt.Printf("    id=%s  server=%s  rtt=%s\n",
		resp.GetRequestId()[:8],
		time.Duration(resp.GetComputationTimeNs()).String(),
		rtt.Round(time.Microsecond),
	)
}

func dialWithRetry(cfg *config.Config, log *logger.Logger) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	for i := 0; i <= cfg.GRPC.MaxRetries; i++ {
		conn, err := grpc.Dial(cfg.GRPC.Address(), opts...)
		if err == nil {
			return conn, nil
		}
		if i < cfg.GRPC.MaxRetries {
			log.Warn("connection failed, retrying", zap.Int("attempt", i+1), zap.Error(err))
			time.Sleep(cfg.GRPC.RetryWaitMin)
		}
	}
	return nil, fmt.Errorf("failed to connect after %d attempts", cfg.GRPC.MaxRetries)
}

func parseTwoArgs(args []string) (float64, float64, error) {
	if len(args) < 2 {
		return 0, 0, fmt.Errorf("need two numeric arguments")
	}
	a, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid first argument %q: %w", args[0], err)
	}
	b, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid second argument %q: %w", args[1], err)
	}
	return a, b, nil
}

func parseOneArg(args []string) (float64, error) {
	if len(args) < 1 {
		return 0, fmt.Errorf("need one numeric argument")
	}
	n, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid argument %q: %w", args[0], err)
	}
	return n, nil
}
