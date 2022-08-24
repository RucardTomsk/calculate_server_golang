package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"

	calculate "calculator/calculate_module"
	pb "calculator/protos"

	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type server struct {
	pb.UnimplementedCalculatingServiceServer
}

func (s *server) Calculate(ctx context.Context, in *pb.Request) (*pb.Response, error) {
	type data_dict_json struct {
		Data_dict     map[string][]bool
		Data_add_dict map[string][]map[string][]map[string]bool
		Weight        map[string]int64
	}

	var st data_dict_json
	byteValue := []byte(in.GetMap())
	err := json.Unmarshal(byteValue, &st)
	if err != nil {
		log.Fatal(err)
	}

	res, plh, plhp := calculate.StartCalculate(int(in.GetIter()), 1, in.GetThreshold(), st.Data_dict, st.Data_add_dict, st.Weight)
	return &pb.Response{Res: res, Plh: plh, Plhp: plhp}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCalculatingServiceServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
