package calculate_module

import (
	"calculator/api_neo4j"
	logmodule "calculator/log_module"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var (
	Labels        = newLabelsRegister()
	Iteration     int
	ProjectScale  int
	Threshold     float64
	Data_dict     map[string][]bool
	Data_add_dict map[string][]map[string][]map[string]bool
	Weight        map[string]int64
	//Mas_calculate_nodes  []string
	Dict_node_name       map[string]string
	Mas_dictionary_nodes []map[string]float64
	Turn                 []string
)

func newLabelsRegister() *LabelsNodesEnum {
	return &LabelsNodesEnum{
		state:                    "state",
		normalVState:             "normalVState",
		checkpoint:               "checkpoint",
		ManagerOpinionCheckpoint: "ManagerOpinionCheckpoint",
		normalVDetail:            "normalVDetail",
		Addcheckpoint:            "Addcheckpoint",
	}
}

type LabelsNodesEnum struct {
	state                    string
	normalVState             string
	checkpoint               string
	ManagerOpinionCheckpoint string
	normalVDetail            string
	Addcheckpoint            string
}

func get_dict_name_nodes(session neo4j.Session) map[string]string {
	mas_checkpoint, err := api_neo4j.Get_all_nodes_the_label(session, Labels.checkpoint)

	if err != nil {
		log.Fatal(err)
	}

	mas_state, err := api_neo4j.Get_all_nodes_the_label(session, Labels.state)

	if err != nil {
		log.Fatal(err)
	}

	var Mas_calculate_nodes []string
	Mas_calculate_nodes = append(Mas_calculate_nodes, mas_checkpoint...)
	Mas_calculate_nodes = append(Mas_calculate_nodes, mas_state...)

	Dict_node_name := make(map[string]string)

	for _, node_guid := range Mas_calculate_nodes {
		name_node, err := api_neo4j.Get_node_name(session, node_guid)
		if err != nil {
			log.Fatal(err)
		}
		Dict_node_name[node_guid] = name_node
	}

	return Dict_node_name
}

func StartCalculate(
	iteration int,
	project_scale int,
	threshold float64,
	data_dict map[string][]bool,
	data_add_dict map[string][]map[string][]map[string]bool,
	weight map[string]int64,
	method_id int64) (string, string, string) {
	//fmt.Println("START")

	Iteration = iteration
	ProjectScale = project_scale
	Threshold = threshold
	Data_dict = data_dict
	Data_add_dict = data_add_dict
	Weight = weight

	session := api_neo4j.GetSession(method_id)
	defer session.Close()
	Dict_node_name = get_dict_name_nodes(session)

	return calculete_node_no_dynemic(session, Iteration)
}

func StartCalculateNextIteration(
	iteration int,
	project_scale int,
	threshold float64,
	data_dict map[string][]bool,
	data_add_dict map[string][]map[string][]map[string]bool,
	weight map[string]int64,
	method_id int64,
	old_iteretion_dict map[string]float64,
	chek_node_guid string,
	flag_change_product bool,
	guid_product string) string {
	//fmt.Println("START")

	Iteration = iteration
	ProjectScale = project_scale
	Threshold = threshold
	Data_dict = data_dict
	Data_add_dict = data_add_dict
	Weight = weight

	session := api_neo4j.GetSession(method_id)
	defer session.Close()
	Dict_node_name = get_dict_name_nodes(session)

	return calculete_node_no_dynemic_next_iteration(session, Iteration, old_iteretion_dict, chek_node_guid, flag_change_product, guid_product)
}

func calculete_node_no_dynemic(session neo4j.Session, number_iteration int) (string, string, string) {
	chanal_lost := logmodule.GetChanal()
	go logmodule.StartLogs(&chanal_lost)
	defer duration(track("calculate"))

	var (
		final_dict      = map[string]float64{}
		final_dictMutex = sync.RWMutex{}
	)
	mas := return_nod_dynemic_data()
	var mas_plh [][]interface{}
	dict_plhp := make(map[string]string)

	type StructDegree struct {
		Name   string
		Degree float64
	}

	chan_calc := make(chan StructDegree)
	for _, node_guid_mas := range mas {
		for _, node_guid_a := range node_guid_mas {
			go func(node_guid string) {
				//fmt.Println(Dict_node_name[node_guid])
				var result_calc StructDegree

				result_calc.Name = Dict_node_name[node_guid]

				flag, _ := api_neo4j.Has_label_node(session, node_guid, Labels.state)

				if flag {
					rezult := 0.0
					final_dictMutex.Lock()
					mas_parents, _ := api_neo4j.Get_node_parents(session, node_guid)
					for _, parent := range mas_parents {
						rezult += 1 / final_dict[Dict_node_name[parent]]
					}
					final_dictMutex.Unlock()
					result_calc.Degree = float64(len(mas_parents)) / rezult
					chan_calc <- result_calc
					return
				}

				Lbase := 1.0
				Rbase := 1.0

				guid_manager_opinion, _ := api_neo4j.Get_node_manager_opinion(session, node_guid)
				key_degree_influence, _ := api_neo4j.Get_degree_influence_node(session, guid_manager_opinion, node_guid)

				degree_influence := Weight[key_degree_influence]

				if Data_dict[node_guid][number_iteration] {
					Lbase *= math.Pow(2, float64(degree_influence))
				} else {
					Rbase *= math.Pow(2, float64(degree_influence)*float64(degree_influence))
				}

				mas_normal_parent, _ := api_neo4j.Get_mas_normal_parents(session, node_guid)

				if len(mas_normal_parent) > 0 {
					for _, parent_guid := range mas_normal_parent {
						//fmt.Println(Dict_node_name[node_guid])
						var K float64
						flagState, err := api_neo4j.Has_label_node(session, parent_guid, Labels.normalVState)
						if err != nil {
							log.Fatal(err)
						}
						flagDetail, err := api_neo4j.Has_label_node(session, parent_guid, Labels.normalVDetail)
						if err != nil {
							log.Fatal(err)
						}
						var N int
						if flagState {
							//fmt.Println("state")
							N = return_N_on_state(parent_guid, number_iteration)
						}
						if flagDetail {
							//fmt.Println("Detail")
							N = return_N_on_details(parent_guid, number_iteration)
						}
						//fmt.Println(N)
						normal, err := api_neo4j.Get_normalValue_node(session, parent_guid)
						if err != nil {
							log.Fatal(err)
						}
						normal_int, err := strconv.Atoi(normal)
						if err != nil {
							log.Fatal(err)
						}
						Z := normal_int * ProjectScale
						_type, err := api_neo4j.Get_type_influence_node(session, parent_guid, node_guid)
						if err != nil {
							log.Fatal(err)
						}
						key_degree_influence_node, err := api_neo4j.Get_degree_influence_node(session, parent_guid, node_guid)
						if err != nil {
							log.Fatal(err)
						}
						degree_influence_node := Weight[key_degree_influence_node]
						//fmt.Println(_type)
						if _type {
							if N == 0 {
								Rbase *= math.Pow(2, float64(degree_influence_node))
							} else if N >= Z {
								K = Log(float64(Z), float64(N))
								Lbase *= K * math.Pow(2, float64(degree_influence_node))
							} else if N < Z {
								K = float64(1 - (1 / (1 + N)))
								Rbase *= (1 - K) * math.Pow(2, float64(degree_influence_node))
								Lbase *= K * math.Pow(2, float64(degree_influence_node))
							}
						} else {
							if N == 0 {

							} else if N >= Z {
								K = Log(float64(Z), float64(N))
								Rbase *= K * math.Pow(2, float64(degree_influence_node)*2)
							} else if N < Z {
								K = float64(N / Z)
								Rbase *= K * math.Pow(2, float64(degree_influence_node)*2)
							}
						}
					}

					//fmt.Print("L-> ", Lbase)
					//fmt.Println("R-> ", Rbase)
				}
				//if len(mas_normal_parent) > 0 {
				//	fmt.Print("F_L-> ", Lbase)
				//	fmt.Println("F_R-> ", Rbase)
				//}
				mas_stat_parents, _ := api_neo4j.Get_mas_stat_parents(session, node_guid)
				if len(mas_stat_parents) == 0 {
					value := Data_dict[node_guid][number_iteration]
					if value {
						result_calc.Degree = 1
					} else {
						result_calc.Degree = 0
					}
					chan_calc <- result_calc
					return
				}
				var LogS strings.Builder
				LogS.WriteString(Dict_node_name[node_guid])
				LogS.WriteString("\n")
				//final_dictMutex.RLock()
				for _, index := range mas_stat_parents {
					LogS.WriteString(Dict_node_name[index])
					LogS.WriteString("->")
					LogS.WriteString(fmt.Sprintf("%f", final_dict[Dict_node_name[index]]))
					LogS.WriteString(" ")
				}
				LogS.WriteString("\n")
				vector_parents := create_vector(len(mas_stat_parents))
				var mas_degree []int64
				mas_degree_parent := map[string]float64{}
				for _, parent := range mas_stat_parents {
					key_degree, err := api_neo4j.Get_degree_influence_node(session, parent, node_guid)
					if err != nil {
						log.Fatal(err)
					}
					degree := Weight[key_degree]
					mas_degree = append(mas_degree, degree)
					mas_degree_parent[parent] = final_dict[Dict_node_name[parent]]
				}
				//final_dictMutex.RUnlock()

				X := 0.0
				//fmt.Println(Lbase, Rbase)
				for _, vector := range vector_parents {
					L := big.NewFloat(Lbase)
					R := big.NewFloat(Rbase)
					Y := big.NewFloat(1)

					for v := 0; v < len(vector); v++ {
						//fmt.Println("TEST")
						if vector[v] == '1' {
							L.Add(L, big.NewFloat(math.Pow(2, float64(mas_degree[v]))))
							Y.Mul(Y, big.NewFloat(mas_degree_parent[mas_stat_parents[v]]))
						} else {
							R.Add(R, big.NewFloat(math.Pow(2, float64(mas_degree[v]))))
							Y.Mul(Y, big.NewFloat(1-mas_degree_parent[mas_stat_parents[v]]))
						}

					}
					L_end, _ := L.Float64()
					R_end, _ := R.Float64()
					Y_end, _ := Y.Float64()
					X += Y_end * (L_end / (L_end + R_end))
					LogS.WriteString(vector)
					LogS.WriteString("\n")
					LogS.WriteString("L -> ")
					LogS.WriteString(fmt.Sprintf("%f", L))
					LogS.WriteString("\n")
					LogS.WriteString("R -> ")
					LogS.WriteString(fmt.Sprintf("%f", R))
					LogS.WriteString("\n")
					LogS.WriteString("Y -> ")
					LogS.WriteString(fmt.Sprintf("%f", Y))
					LogS.WriteString("\n")
					LogS.WriteString("X -> ")
					LogS.WriteString(fmt.Sprintf("%f", X))
					LogS.WriteString("\n")
				}

				result_calc.Degree = X

				/*if X < Threshold {
					mas := []interface{}{Dict_node_name[node_guid], X}
					mas_plh = append(mas_plh, mas)

					var str_parent []string
					mas_parents, _ := api_neo4j.Get_node_parents(session, node_guid)
					for _, parent := range mas_parents {
						flag, _ := api_neo4j.Has_label_node(session, parent, Labels.checkpoint)
						if flag {
							if !Data_dict[parent][Iteration] {
								str_parent = append(str_parent, Dict_node_name[parent])
							}
						}

					}
					dict_plhp[Dict_node_name[node_guid]] = strings.Join(str_parent, ",")
				}
				*/

				chan_calc <- result_calc
				chanal_lost <- LogS.String()
			}(node_guid_a)
		}
		counter := 0
		for {
			a := <-chan_calc
			final_dictMutex.Lock()
			final_dict[a.Name] = a.Degree
			final_dictMutex.Unlock()
			counter++
			if counter == len(node_guid_mas) {
				break
			}
		}

	}
	dictOutPlhAndPlhp := make(map[string]interface{})
	dictOutPlhAndPlhp["plh"] = mas_plh
	dictOutPlhAndPlhp["plhp"] = dict_plhp

	f_json, err := json.MarshalIndent(&final_dict, "", "   ")
	if err != nil {
		panic(err)
	}
	p_json, err := json.MarshalIndent(&mas_plh, "", "   ")
	if err != nil {
		panic(err)
	}
	pp_json, err := json.MarshalIndent(&dict_plhp, "", "   ")
	if err != nil {
		panic(err)
	}
	//fmt.Println(string(f_json))
	logmodule.StopLogs(&chanal_lost)
	return string(f_json), string(p_json), string(pp_json)

}

func calculete_node_no_dynemic_next_iteration(session neo4j.Session, number_iteration int, old_dict_iteration map[string]float64, chek_node_guid string, flag_change_product bool, guid_product string) string {
	//f, _ := os.OpenFile("info.log", os.O_RDWR|os.O_CREATE, 0666)

	//defer f.Close()

	//infoLog := log.New(f, "INFO\t", log.Ldate|log.Ltime)

	//defer duration(track("calculate"))

	var (
		final_dict      = map[string]float64{}
		final_dictMutex = sync.RWMutex{}
	)
	mas := return_nod_dynemic_data()
	var mas_f [][]string
	if !flag_change_product {
		flag_m := true
		for _, node_mas := range mas {
			for _, cos_guid_node := range node_mas {
				if cos_guid_node == chek_node_guid {
					flag_m = false
				}
			}
			if flag_m {
				for _, cos_guid_node := range node_mas {
					final_dict[Dict_node_name[cos_guid_node]] = old_dict_iteration[Dict_node_name[cos_guid_node]]
				}
			} else {
				mas_f = append(mas_f, node_mas)
			}

		}
	} else {
		mas_f = mas
	}

	type StructDegree struct {
		Name   string
		Degree float64
	}

	chan_calc := make(chan StructDegree)
	for _, node_guid_mas := range mas_f {
		for _, node_guid_a := range node_guid_mas {
			go func(node_guid string) {
				//fmt.Println(Dict_node_name[node_guid])
				var result_calc StructDegree

				result_calc.Name = Dict_node_name[node_guid]

				flag, _ := api_neo4j.Has_label_node(session, node_guid, Labels.state)

				if flag {
					rezult := 0.0
					final_dictMutex.Lock()
					mas_parents, _ := api_neo4j.Get_node_parents(session, node_guid)
					for _, parent := range mas_parents {
						rezult += 1 / final_dict[Dict_node_name[parent]]
					}
					final_dictMutex.Unlock()
					result_calc.Degree = float64(len(mas_parents)) / rezult
					chan_calc <- result_calc
					return
				}

				Lbase := 1.0
				Rbase := 1.0

				guid_manager_opinion, _ := api_neo4j.Get_node_manager_opinion(session, node_guid)
				key_degree_influence, _ := api_neo4j.Get_degree_influence_node(session, guid_manager_opinion, node_guid)

				degree_influence := Weight[key_degree_influence]

				if Data_dict[node_guid][number_iteration] {
					Lbase *= math.Pow(2, float64(degree_influence))
				} else {
					Rbase *= math.Pow(2, float64(degree_influence)*float64(degree_influence))
				}

				mas_normal_parent, _ := api_neo4j.Get_mas_normal_parents(session, node_guid)

				if len(mas_normal_parent) > 0 {
					for _, parent_guid := range mas_normal_parent {
						//fmt.Println(Dict_node_name[node_guid])
						var K float64
						flagState, err := api_neo4j.Has_label_node(session, parent_guid, Labels.normalVState)
						if err != nil {
							log.Fatal(err)
						}
						flagDetail, err := api_neo4j.Has_label_node(session, parent_guid, Labels.normalVDetail)
						if err != nil {
							log.Fatal(err)
						}
						var N int
						if flagState {
							//fmt.Println("state")
							N = return_N_on_state(parent_guid, number_iteration)
						}
						if flagDetail {
							//fmt.Println("Detail")
							N = return_N_on_details(parent_guid, number_iteration)
						}
						if flag_change_product {
							if parent_guid == guid_product {
								N += 1
							}
						}
						//fmt.Println(N)
						normal, err := api_neo4j.Get_normalValue_node(session, parent_guid)
						if err != nil {
							log.Fatal(err)
						}
						normal_int, err := strconv.Atoi(normal)
						if err != nil {
							log.Fatal(err)
						}
						Z := normal_int * ProjectScale
						_type, err := api_neo4j.Get_type_influence_node(session, parent_guid, node_guid)
						if err != nil {
							log.Fatal(err)
						}
						key_degree_influence_node, err := api_neo4j.Get_degree_influence_node(session, parent_guid, node_guid)
						if err != nil {
							log.Fatal(err)
						}
						degree_influence_node := Weight[key_degree_influence_node]
						//fmt.Println(_type)
						if _type {
							if N == 0 {
								Rbase *= math.Pow(2, float64(degree_influence_node))
							} else if N >= Z {
								K = Log(float64(Z), float64(N))
								Lbase *= K * math.Pow(2, float64(degree_influence_node))
							} else if N < Z {
								K = float64(1 - (1 / (1 + N)))
								Rbase *= (1 - K) * math.Pow(2, float64(degree_influence_node))
								Lbase *= K * math.Pow(2, float64(degree_influence_node))
							}
						} else {
							if N == 0 {

							} else if N >= Z {
								K = Log(float64(Z), float64(N))
								Rbase *= K * math.Pow(2, float64(degree_influence_node)*2)
							} else if N < Z {
								K = float64(N / Z)
								Rbase *= K * math.Pow(2, float64(degree_influence_node)*2)
							}
						}
					}

					//fmt.Print("L-> ", Lbase)
					//fmt.Println("R-> ", Rbase)
				}
				//if len(mas_normal_parent) > 0 {
				//	fmt.Print("F_L-> ", Lbase)
				//	fmt.Println("F_R-> ", Rbase)
				//}
				mas_stat_parents, _ := api_neo4j.Get_mas_stat_parents(session, node_guid)
				if len(mas_stat_parents) == 0 {
					value := Data_dict[node_guid][number_iteration]
					if value {
						result_calc.Degree = 1
					} else {
						result_calc.Degree = 0
					}
					chan_calc <- result_calc
					return
				}
				//var LogS string
				//LogS += Dict_node_name[node_guid] + "\n"
				final_dictMutex.RLock()
				//for _, index := range mas_stat_parents {
				//	LogS += Dict_node_name[index] + "->" + fmt.Sprintf("%f", final_dict[Dict_node_name[index]]) + " "
				//}
				//LogS += "\n"
				vector_parents := create_vector(len(mas_stat_parents))
				var mas_degree []int64
				mas_degree_parent := map[string]float64{}
				for _, parent := range mas_stat_parents {
					key_degree, err := api_neo4j.Get_degree_influence_node(session, parent, node_guid)
					if err != nil {
						log.Fatal(err)
					}
					degree := Weight[key_degree]
					mas_degree = append(mas_degree, degree)
					mas_degree_parent[parent] = final_dict[Dict_node_name[parent]]
				}
				final_dictMutex.RUnlock()

				X := 0.0
				//fmt.Println(Lbase, Rbase)
				for _, vector := range vector_parents {
					L := big.NewFloat(Lbase)
					R := big.NewFloat(Rbase)
					Y := big.NewFloat(1)

					for v := 0; v < len(vector); v++ {
						//fmt.Println("TEST")
						if vector[v] == '1' {
							L.Add(L, big.NewFloat(math.Pow(2, float64(mas_degree[v]))))
							Y.Mul(Y, big.NewFloat(mas_degree_parent[mas_stat_parents[v]]))
						} else {
							R.Add(R, big.NewFloat(math.Pow(2, float64(mas_degree[v]))))
							Y.Mul(Y, big.NewFloat(1-mas_degree_parent[mas_stat_parents[v]]))
						}

					}
					L_end, _ := L.Float64()
					R_end, _ := R.Float64()
					Y_end, _ := Y.Float64()
					X += Y_end * (L_end / (L_end + R_end))
					//LogS += vector + "\n"
					//LogS += "L -> " + fmt.Sprintf("%f", L) + "\n"
					//LogS += "R -> " + fmt.Sprintf("%f", R) + "\n"
					//LogS += "Y -> " + fmt.Sprintf("%f", Y) + "\n"
					//LogS += "X -> " + fmt.Sprintf("%f", X) + "\n"
				}

				result_calc.Degree = X

				/*if X < Threshold {
					mas := []interface{}{Dict_node_name[node_guid], X}
					mas_plh = append(mas_plh, mas)

					var str_parent []string
					mas_parents, _ := api_neo4j.Get_node_parents(session, node_guid)
					for _, parent := range mas_parents {
						flag, _ := api_neo4j.Has_label_node(session, parent, Labels.checkpoint)
						if flag {
							if !Data_dict[parent][Iteration] {
								str_parent = append(str_parent, Dict_node_name[parent])
							}
						}

					}
					dict_plhp[Dict_node_name[node_guid]] = strings.Join(str_parent, ",")
				}
				*/

				chan_calc <- result_calc
				//infoLog.Println("\n" + LogS)
			}(node_guid_a)
		}
		counter := 0
		for {
			a := <-chan_calc
			final_dictMutex.Lock()
			final_dict[a.Name] = a.Degree
			final_dictMutex.Unlock()
			counter++
			if counter == len(node_guid_mas) {
				break
			}
		}

	}

	f_json, err := json.MarshalIndent(&final_dict, "", "   ")
	if err != nil {
		panic(err)
	}

	//fmt.Println(string(f_json))
	return string(f_json)

}

func track(msg string) (string, time.Time) {
	return msg, time.Now()
}

func duration(msg string, start time.Time) {
	fmt.Printf("%v: %v\n", msg, time.Since(start))
}

func return_N_on_state(parent_guid string, number_iteretion int) int {
	counter := 0
	for _, copy_state := range Data_add_dict[parent_guid] {
		counter_flag := 0
		for key := range copy_state {
			for _, iter := range copy_state[key] {
				flag, ok := iter[fmt.Sprint(number_iteretion)]
				if ok {
					if flag {
						counter_flag += 1
						break
					}
				}
			}
		}
		if counter_flag == len(copy_state) {
			counter += 1
		}
	}
	return counter
}

func return_N_on_details(parent_guid string, number_iteretion int) int {
	counter := 0
	for _, copy_datail := range Data_add_dict[parent_guid] {
		for key := range copy_datail {
			flag := false
			for _, iter := range copy_datail[key] {
				flag, ok := iter[fmt.Sprint(number_iteretion)]
				if ok {
					if flag {
						counter += 1
						flag = false
						break
					}
				}
			}
			if flag {
				break
			}
		}
	}
	return counter
}

func create_vector(len_vector int) []string {
	var mas []string
	for i := 0; i < int(math.Pow(2, float64(len_vector))); i++ {
		bin_value, _ := ConvertInt(fmt.Sprint(i), 10, 2)
		flag := len(bin_value)
		for flag != len_vector {
			bin_value = "0" + bin_value
			flag = len(bin_value)
		}
		mas = append(mas, bin_value)
	}
	return mas
}

func ConvertInt(val string, base, toBase int) (string, error) {
	i, err := strconv.ParseInt(val, base, 64)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(i, toBase), nil
}

func Log(base, x float64) float64 {
	return math.Log(x) / math.Log(base)
}

func return_nod_dynemic_data() [][]string {
	type json_struct struct {
		Mas [][]string
	}

	var st json_struct
	byteValue, err := ioutil.ReadFile("calculate_order.json")
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(byteValue, &st)
	if err != nil {
		log.Fatal(err)
	}
	//fmt.Println(st.Mas)
	return st.Mas
}
