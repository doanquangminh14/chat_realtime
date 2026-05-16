package middleware

import (
	"context"
	"time"

	"github.com/distributed-systems/internal/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryLoggingInterceptor logs each unary RPC call with structured fields
func UnaryLoggingInterceptor(log *logger.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		// Extract request ID from metadata if present
		requestID := extractRequestID(ctx)
		callLog := log.With(
			zap.String("grpc_method", info.FullMethod),
			zap.String("request_id", requestID),
		)

		callLog.Info("gRPC request received")

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}

		fields := []zap.Field{
			zap.String("grpc_method", info.FullMethod),
			zap.String("grpc_code", code.String()),
			zap.Duration("duration", duration),
			zap.String("request_id", requestID),
		}

		if err != nil {
			callLog.Error("gRPC request failed", append(fields, zap.Error(err))...)
		} else {
			callLog.Info("gRPC request completed", fields...)
		}

		return resp, err
	}
}

// UnaryRecoveryInterceptor recovers from panics in handlers
func UnaryRecoveryInterceptor(log *logger.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("gRPC handler panic recovered",
					zap.String("method", info.FullMethod),
					zap.Any("panic", r),
				)
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

// UnaryMetricsInterceptor collects basic metrics (can be extended with Prometheus)
func UnaryMetricsInterceptor(log *logger.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}

		// In production, emit to Prometheus/OpenTelemetry here
		log.Debug("metrics recorded",
			zap.String("method", info.FullMethod),
			zap.String("code", code.String()),
			zap.Duration("latency", duration),
		)

		return resp, err
	}
}

// ChainUnaryInterceptors chains multiple unary interceptors
func ChainUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		chain := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chain
			chain = func(ctx context.Context, req interface{}) (interface{}, error) {
				return interceptor(ctx, req, info, next)
			}
		}
		return chain(ctx, req)
	}
}

func extractRequestID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get("x-request-id")
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
