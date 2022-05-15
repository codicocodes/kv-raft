coordinator:
	touch mr-out-0
	rm mr-out*
	go run -race ./mrcoordinator.go ./pg*.txt

worker:
	go build -race -buildmode=plugin ../mrapps/wc.go
	go run -race mrworker.go wc.so

crash:
	go build -race -buildmode=plugin ../mrapps/crash.go
	go run -race mrworker.go crash.so

seq:
	touch mr-out-0
	go build -race -buildmode=plugin ../mrapps/wc.go
	rm mr-out*
	go run -race ./mrsequential.go wc.so ./pg*.txt
