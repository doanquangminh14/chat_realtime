package handler

import (
	"context"
	"time"

	"github.com/distributed-systems/internal/logger"
	calculatorpb "github.com/distributed-systems/internal/rpc/proto"
	"github.com/distributed-systems/internal/rpc/service"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	statusSuccess = "success"
	statusError   = "error"
	appVersion    = "1.0.0"
)

// CalculatorHandler implements the gRPC CalculatorServiceServer interface.
type CalculatorHandler struct {
	calculatorpb.UnimplementedCalculatorServiceServer
	svc *service.CalculatorService
	log *logger.Logger
}

func NewCalculatorHandler(svc *service.CalculatorService, log *logger.Logger) *CalculatorHandler {
	return &CalculatorHandler{
		svc: svc,
		log: log.WithComponent("grpc-handler"),
	}
}

func (h *CalculatorHandler) Add(ctx context.Context, req *calculatorpb.BinaryRequest) (*calculatorpb.CalculationResponse, error) {
	if err := h.validateBinary(req); err != nil {
		return nil, err
	}

	start := time.Now()

	result, err := h.svc.Add(
		ctx,
		req.GetRequestId(),
		req.GetOperandA(),
		req.GetOperandB(),
	)

	return h.buildResponse(
		req.GetRequestId(),
		"add",
		result,
		[]float64{req.GetOperandA(), req.GetOperandB()},
		start,
		err,
	)
}

func (h *CalculatorHandler) Subtract(ctx context.Context, req *calculatorpb.BinaryRequest) (*calculatorpb.CalculationResponse, error) {
	if err := h.validateBinary(req); err != nil {
		return nil, err
	}

	start := time.Now()

	result, err := h.svc.Subtract(
		ctx,
		req.GetRequestId(),
		req.GetOperandA(),
		req.GetOperandB(),
	)

	return h.buildResponse(
		req.GetRequestId(),
		"subtract",
		result,
		[]float64{req.GetOperandA(), req.GetOperandB()},
		start,
		err,
	)
}

func (h *CalculatorHandler) Multiply(ctx context.Context, req *calculatorpb.BinaryRequest) (*calculatorpb.CalculationResponse, error) {
	if err := h.validateBinary(req); err != nil {
		return nil, err
	}

	start := time.Now()

	result, err := h.svc.Multiply(
		ctx,
		req.GetRequestId(),
		req.GetOperandA(),
		req.GetOperandB(),
	)

	return h.buildResponse(
		req.GetRequestId(),
		"multiply",
		result,
		[]float64{req.GetOperandA(), req.GetOperandB()},
		start,
		err,
	)
}

func (h *CalculatorHandler) Divide(ctx context.Context, req *calculatorpb.BinaryRequest) (*calculatorpb.CalculationResponse, error) {
	if err := h.validateBinary(req); err != nil {
		return nil, err
	}

	start := time.Now()

	result, err := h.svc.Divide(
		ctx,
		req.GetRequestId(),
		req.GetOperandA(),
		req.GetOperandB(),
	)

	return h.buildResponse(
		req.GetRequestId(),
		"divide",
		result,
		[]float64{req.GetOperandA(), req.GetOperandB()},
		start,
		err,
	)
}

func (h *CalculatorHandler) Power(ctx context.Context, req *calculatorpb.BinaryRequest) (*calculatorpb.CalculationResponse, error) {
	if err := h.validateBinary(req); err != nil {
		return nil, err
	}

	start := time.Now()

	result, err := h.svc.Power(
		ctx,
		req.GetRequestId(),
		req.GetOperandA(),
		req.GetOperandB(),
	)

	return h.buildResponse(
		req.GetRequestId(),
		"power",
		result,
		[]float64{req.GetOperandA(), req.GetOperandB()},
		start,
		err,
	)
}

func (h *CalculatorHandler) Factorial(ctx context.Context, req *calculatorpb.UnaryRequest) (*calculatorpb.CalculationResponse, error) {
	if err := h.validateUnary(req); err != nil {
		return nil, err
	}

	start := time.Now()

	result, err := h.svc.Factorial(
		ctx,
		req.GetRequestId(),
		req.GetN(),
	)

	return h.buildResponse(
		req.GetRequestId(),
		"factorial",
		result,
		[]float64{float64(req.GetN())},
		start,
		err,
	)
}

func (h *CalculatorHandler) Fibonacci(ctx context.Context, req *calculatorpb.UnaryRequest) (*calculatorpb.CalculationResponse, error) {
	if err := h.validateUnary(req); err != nil {
		return nil, err
	}

	start := time.Now()

	result, err := h.svc.Fibonacci(
		ctx,
		req.GetRequestId(),
		req.GetN(),
	)

	return h.buildResponse(
		req.GetRequestId(),
		"fibonacci",
		result,
		[]float64{float64(req.GetN())},
		start,
		err,
	)
}

func (h *CalculatorHandler) HealthCheck(_ context.Context, req *calculatorpb.HealthRequest) (*calculatorpb.HealthResponse, error) {
	h.log.Info(
		"health check",
		zap.String("service", req.GetService()),
	)

	return &calculatorpb.HealthResponse{
		Status:    "healthy",
		Version:   appVersion,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (h *CalculatorHandler) validateBinary(req *calculatorpb.BinaryRequest) error {
	if req == nil {
		return status.Error(
			codes.InvalidArgument,
			"request must not be nil",
		)
	}

	if req.GetRequestId() == "" {
		return status.Error(
			codes.InvalidArgument,
			"request_id is required",
		)
	}

	return nil
}

func (h *CalculatorHandler) validateUnary(req *calculatorpb.UnaryRequest) error {
	if req == nil {
		return status.Error(
			codes.InvalidArgument,
			"request must not be nil",
		)
	}

	if req.GetRequestId() == "" {
		return status.Error(
			codes.InvalidArgument,
			"request_id is required",
		)
	}

	return nil
}

func (h *CalculatorHandler) buildResponse(
	requestID string,
	operation string,
	result float64,
	operands []float64,
	start time.Time,
	err error,
) (*calculatorpb.CalculationResponse, error) {

	elapsed := time.Since(start)

	if err != nil {
		h.log.Warn(
			"calculation error",
			zap.String("op", operation),
			zap.String("rid", requestID),
			zap.Error(err),
		)

		return &calculatorpb.CalculationResponse{
			RequestId:         requestID,
			Operation:         operation,
			Operands:          operands,
			Timestamp:         timestamppb.New(time.Now()),
			ComputationTimeNs: elapsed.Nanoseconds(),
			Status:            statusError,
			ErrorMessage:      err.Error(),
		}, status.Errorf(
			codes.InvalidArgument,
			"%s",
			err.Error(),
		)
	}

	return &calculatorpb.CalculationResponse{
		RequestId:         requestID,
		Operation:         operation,
		Result:            result,
		Operands:          operands,
		Timestamp:         timestamppb.New(time.Now()),
		ComputationTimeNs: elapsed.Nanoseconds(),
		Status:            statusSuccess,
	}, nil
}