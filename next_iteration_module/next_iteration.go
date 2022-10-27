package next_iteration

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	api "calculator/api_neo4j"
	calculate "calculator/calculate_module"
)

func NextIteration(
	iteration int,
	project_scale int,
	threshold float64,
	data_dict map[string][]bool,
	data_add_dict map[string][]map[string][]map[string]bool,
	weight map[string]int64,
	method_id int64,
	old_dict_iteration map[string]float64) (string, float64) {
	defer duration(track("NextIteration"))
	fin_dict := make(map[string]float64)
	flag_counter := 0
	for _, value := range data_dict {
		if !value[iteration] {
			flag_counter++
		}
	}
	flag_chan := make(chan bool)
	session := api.GetSession(method_id)
	defer session.Close()
	mas_product, _ := api.Get_mas_normal_parents_concrect_projectr(session)
	flag_counter += len(mas_product)
	for name_node, _ := range data_dict {
		//fmt.Println(name_node)
		if !data_dict[name_node][iteration] {
			go func(name_node string) {
				intermediate_dict := make(map[string][]bool)
				for key, value := range data_dict {
					//fmt.Println("TEST1", value)
					_mas := make([]bool, len(value))
					copy(_mas, value)
					//fmt.Println("TEST2", _mas)
					intermediate_dict[key] = _mas
				}
				intermediate_dict[name_node][iteration] = true
				fin_dict[name_node] = GetDelta(calculate.StartCalculateNextIteration(iteration, project_scale, threshold, intermediate_dict, data_add_dict, weight, method_id, old_dict_iteration, name_node, false, ""))
				flag_chan <- true
			}(name_node)
		}
	}
	for _, guid_product := range mas_product {
		go func(guid_product string) {
			fin_dict[guid_product] = GetDelta(calculate.StartCalculateNextIteration(iteration, project_scale, threshold, data_dict, data_add_dict, weight, method_id, old_dict_iteration, "", true, guid_product))
			flag_chan <- true
		}(guid_product)
	}

	flag_counter_2 := 0
	for {
		<-flag_chan
		flag_counter_2++
		if flag_counter_2 == flag_counter {
			break
		}
	}
	return (GetAlfaNode(fin_dict, method_id))
}

func GetDelta(res string) float64 {
	_map := make(map[string]float64)

	//fmt.Println(res)
	json.Unmarshal([]byte(res), &_map)
	counter := 0.0
	for _, value := range _map {
		counter += math.Round(value*100) / 100
	}

	return counter / float64(len(_map))
}

func GetAlfaNode(IM map[string]float64, method_id int64) (string, float64) {
	type end_struct struct {
		name_node string
		value     float64
	}
	//fmt.Println(IM)
	var ES end_struct
	ES.value = 0
	for node_name, value := range IM {
		if value > ES.value {
			ES.name_node = node_name
			ES.value = value
		}
	}

	session := api.GetSession(method_id)
	defer session.Close()
	name, _ := api.Get_node_name(session, ES.name_node)
	return name, ES.value
}

func track(msg string) (string, time.Time) {
	return msg, time.Now()
}

func duration(msg string, start time.Time) {
	fmt.Printf("%v: %v\n", msg, time.Since(start))
}
