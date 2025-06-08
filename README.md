# Kratos Project Template

## 项目介绍
基于go-kratos的评价评论系统，整个评价服务包含C端、B端和O端三部分，涉及C端用户发表评价，O端运营审核评价以及B端商家查看/回复评价等主要业务流程。
该部分为一个独立的微服务中台项目。
其他端详见https://github.com/HayesYu/go_kratos_review_o; https://github.com/HayesYu/go_kratos_review_b; https://github.com/HayesYu/go_kratos_review_task

## review-service端
![image](https://github.com/user-attachments/assets/7bdcdfc2-ced3-4144-b105-f4555db6de0e)


## Install Kratos
```
go install github.com/go-kratos/kratos/cmd/kratos/v2@latest
```
## Create a service
```
# Create a template project
kratos new server

cd server
# Add a proto template
kratos proto add api/server/server.proto
# Generate the proto code
kratos proto client api/server/server.proto
# Generate the source code of service by proto file
kratos proto server api/server/server.proto -t internal/service

go generate ./...
go build -o ./bin/ ./...
./bin/server -conf ./configs
```
## Generate other auxiliary files by Makefile
```
# Download and update dependencies
make init
# Generate API files (include: pb.go, http, grpc, validate, swagger) by proto file
make api
# Generate all files
make all
```
## Automated Initialization (wire)
```
# install wire
go get github.com/google/wire/cmd/wire

# generate wire
cd cmd/server
wire
```

## Docker
```bash
# build
docker build -t <your-docker-image-name> .

# run
docker run --rm -p 8000:8000 -p 9000:9000 -v </path/to/your/configs>:/data/conf <your-docker-image-name>
```

