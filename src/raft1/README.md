# Raft实验说明
1. 运行可以通过 `VERBOSE=1 go test -run 3A -race` 执行测试
2. 运行结果可以通过 `./dslogs -c 7 src/raft1/3A.log` 分析结果
3. 大规模运行可通过 `../../dstest -v -p 32 -n 2000 -r 3A` 得到结果(在src/raft1目录下运行)
