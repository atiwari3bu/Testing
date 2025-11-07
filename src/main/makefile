open_rpc:
	vim ../mr/rpc.go
open_worker:
	vim ../mr/worker.go
run_worker:
	go build -buildmode=plugin ../mrapps/wc.go
	go run mrworker.go wc.so
open_coord:
	vim ../mr/coordinator.go
run_coord:
	go run mrcoordinator.go pg-*.txt
