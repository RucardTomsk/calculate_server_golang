import HTTP_API_neo4j
import enum
import json
from array import *

class LabelsNodesEnum(enum.Enum):
    state = "state"
    normalVState = "normalVState"
    checkpoint = "checkpoint"
    ManagerOpinionCheckpoint = "ManagerOpinionCheckpoint"
    normalVDetail = "normalVDetail"
    Addcheckpoint = "Addcheckpoint"
    DynamicCheckpoint = "DynamicCheckpoint"


class create_order(object):
    def __init__(self,name_database) -> None:
        self.API = HTTP_API_neo4j.API(name_database)
        self.MAS_NODES  = self.get_nodes()
        self.DICT_TEST = self.get_dict_test()
        self.mas_order = []
        self.train = []
        pass

    def get_dict_test(self):
        return {node_guid: False for node_guid in self.MAS_NODES}

    def get_nodes(self):
        mas = self.API.get_all_nodes_the_label(LabelsNodesEnum.checkpoint.value) + self.API.get_all_nodes_the_label(LabelsNodesEnum.state.value)
        #for i in range(len(mas)-1):
        #    for node_index in range(len(mas)-2):
        #        if len(self.API.get_node_parents(mas[node_index])) < len(self.API.get_node_parents(mas[node_index+1])):
        #            a = mas[node_index]
        #            mas[node_index] = mas[node_index+1]
        #            mas[node_index+1] = a

        return mas

    def referenc(self,node_guid,order):
        if node_guid in self.train or self.DICT_TEST[node_guid] == True:
            return

        self.train.append(node_guid)

        mas_parent = self.API.get_node_parents(node_guid)
        for guid_parent in mas_parent:
            if(self.API.has_label_node(guid_parent,LabelsNodesEnum.checkpoint.value) or self.API.has_label_node(guid_parent,LabelsNodesEnum.state.value)):
                self.referenc(guid_parent,order+1)

        self.mas_order.append(node_guid)
        self.DICT_TEST[node_guid] = True

    def main(self):
        for node_guid in self.MAS_NODES:
            self.referenc(node_guid,0)

        order = self.mas_order

        order_arr = array('i',[index for index in range(len(order))])

        end_mas = []

        while len(order_arr) != 0:
            mas1 = []
            mas2 = []
            while len(order_arr) != 0:
                mas1.append(order_arr[0])
                mas = [order.index(node_guid) for node_guid in self.API.return_addiction(order[order_arr[0]])]
                order_arr.remove(order_arr[0])
                for node in mas:
                    if node in mas2:
                        pass
                    else:
                        order_arr.remove(node)
                        mas2.append(node)
                        #print("node_guid")
                        #print(node_id)
                        #print(node)
                
                for node_id_2 in mas2:
                        mas = [order.index(node_guid) for node_guid in self.API.return_addiction(order[node_id_2])]
                        for node in mas:
                            if node in mas2:
                                pass
                            else:
                                order_arr.remove(node)
                                mas2.append(node)

                #print(order_arr)

            mas2.sort()
            order_arr = array('i',mas2)

            end_mas.append([order[node_id] for node_id in mas1])

            print([self.API.get_node_name(order[node_id]) for node_id in mas1])
            print([self.API.get_node_name(order[node_id]) for node_id in mas2])

        return end_mas

if __name__ == "__main__":
    get_order = create_order("prectice")
    end_mas = get_order.main()

    with open("calculate_order.json",'w') as f:
        #newMas = []
        #for m in end_mas:
            #newMas += m
        f.write(json.dumps({'mas':end_mas}, indent = 4))
