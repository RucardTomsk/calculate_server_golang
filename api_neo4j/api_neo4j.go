package api_neo4j

import (
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var (
	Ndriver, _ = neo4j.NewDriver("bolt://127.0.0.1:7687", neo4j.BasicAuth("bot", "contrelspawn123", ""))
)

func Get_guid_all_nodes(session neo4j.Session) ([]string, error) {
	mas_name, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		var list []string
		result, err := tx.Run("MATCH (n) RETURN n.guid", nil)
		if err != nil {
			return nil, err
		}
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		if err = result.Err(); err != nil {
			return nil, err
		}
		return list, nil
	})
	if err != nil {
		return nil, err
	}
	//time.Sleep(time.Microsecond)
	return mas_name.([]string), err
}

func Get_node_name(session neo4j.Session, guid string) (string, error) {
	node, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run("MATCH (n) WHERE n.guid = $guid RETURN n.name", map[string]interface{}{
			"guid": guid,
		})
		if err != nil {
			return nil, err
		}
		if result.Next() {
			return result.Record().Values[0], nil
		}
		return nil, err
	})
	if err != nil {
		return "", err
	}
	//time.Sleep(time.Microsecond)
	return node.(string), nil
}

func Get_node_manager_opinion(session neo4j.Session, guid string) (string, error) {
	node, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run("MATCH (y:ManagerOpinionCheckpoint)-[]->(n) WHERE n.guid = $guid RETURN y.guid", map[string]interface{}{
			"guid": guid,
		})
		if err != nil {
			return nil, err
		}
		if result.Next() {
			return result.Record().Values[0], nil
		}
		return nil, err
	})
	if err != nil {
		return "", err
	}
	//time.Sleep(time.Microsecond)
	return node.(string), nil
}

func Get_node_children(session neo4j.Session, guid string) ([]string, error) {
	parents, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		var list []string
		result, err := tx.Run("MATCH (n)-[]->(p) WHERE n.guid = $guid RETURN p.guid", map[string]interface{}{
			"guid": guid,
		})
		if err != nil {
			return nil, err
		}
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		if err = result.Err(); err != nil {
			return nil, err
		}
		//list[0], list[1] = list[1], list[0]
		return list, nil
	})
	if err != nil {
		return nil, err
	}
	//time.Sleep(time.Microsecond)
	return parents.([]string), nil
}

func Get_node_parents(session neo4j.Session, guid string) ([]string, error) {
	parents, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		var list []string
		result, err := tx.Run("MATCH (p)-[]->(n) WHERE n.guid = $guid RETURN p.guid", map[string]interface{}{
			"guid": guid,
		})
		if err != nil {
			return nil, err
		}
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		if err = result.Err(); err != nil {
			return nil, err
		}
		//list[0], list[1] = list[1], list[0]
		return list, nil
	})
	if err != nil {
		return nil, err
	}
	//time.Sleep(time.Microsecond)
	return parents.([]string), nil
}

func Get_count_normal(session neo4j.Session) int64 {
	counter := 0
	session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, _ := tx.Run("MATCH (p:normalVState)-[]->(:checkpoint) RETURN DISTINCT n.guid", map[string]interface{}{})
		for result.Next() {
			counter++
		}
		result, _ = tx.Run("MATCH (p:normalVDetail)-[]->(:checkpoint) RETURN DISTINCT n.guid", map[string]interface{}{})
		for result.Next() {
			counter++
		}

		return counter, nil
	})

	return 0
}

func Get_node_parents_labels(session neo4j.Session, guid string, label string) ([]string, error) {
	mas_parents, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		var list []string
		request := "MATCH (p:" + label + ")-[]->(n) WHERE n.guid = $guid RETURN p.guid"
		result, err := tx.Run(request, map[string]interface{}{
			"guid": guid,
		})
		if err != nil {
			return nil, err
		}
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		if err = result.Err(); err != nil {
			return nil, err
		}
		return list, nil
	})
	if err != nil {
		return nil, err
	}
	//time.Sleep(time.Microsecond)
	return mas_parents.([]string), nil
}

func Get_mas_normal_parents(session neo4j.Session, guid string) ([]string, error) {
	mas_parents, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		var list []string
		request := "MATCH (p:normalVDetail)-[]->(n) WHERE n.guid = $guid RETURN p.guid"
		result, err := tx.Run(request, map[string]interface{}{
			"guid": guid,
		})
		if err != nil {
			return nil, err
		}
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		request = "MATCH (p:normalVState)-[]->(n) WHERE n.guid = $guid RETURN p.guid"
		result, err = tx.Run(request, map[string]interface{}{
			"guid": guid,
		})
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		if err = result.Err(); err != nil {
			return nil, err
		}
		return list, nil
	})
	if err != nil {
		return nil, err
	}
	//time.Sleep(time.Microsecond)
	return mas_parents.([]string), nil
}

func Get_mas_normal_parents_concrect_projectr(session neo4j.Session) ([]string, error) {
	mas_parents, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		var list []string
		request := "MATCH (p:normalVDetail)-[]->(:checkpoint) RETURN DISTINCT p.guid"
		result, err := tx.Run(request, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		request = "MATCH (p:normalVState)-[]->(:checkpoint) RETURN DISTINCT p.guid"
		result, err = tx.Run(request, map[string]interface{}{})
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		if err = result.Err(); err != nil {
			return nil, err
		}
		return list, nil
	})
	if err != nil {
		return nil, err
	}
	//time.Sleep(time.Microsecond)
	return mas_parents.([]string), nil
}

func Get_mas_stat_parents(session neo4j.Session, guid string) ([]string, error) {
	mas_parents, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		var list []string
		request := "MATCH (p:checkpoint)-[]->(n) WHERE n.guid = $guid RETURN p.guid"
		result, err := tx.Run(request, map[string]interface{}{
			"guid": guid,
		})
		if err != nil {
			return nil, err
		}
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		request = "MATCH (p:state)-[]->(n) WHERE n.guid = $guid RETURN p.guid"
		result, err = tx.Run(request, map[string]interface{}{
			"guid": guid,
		})
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		if err = result.Err(); err != nil {
			return nil, err
		}
		return list, nil
	})
	if err != nil {
		return nil, err
	}
	//time.Sleep(time.Microsecond)
	return mas_parents.([]string), nil
}

func Has_label_node(session neo4j.Session, guid string, label string) (bool, error) {
	node, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run("MATCH (n) WHERE n.guid = $guid RETURN labels(n)", map[string]interface{}{
			"guid": guid,
		})
		if err != nil {
			return nil, err
		}
		if result.Next() {
			return result.Record().Values[0], nil
		}
		return nil, err
	})
	if err != nil {
		return true, err
	}
	//time.Sleep(time.Microsecond)
	if node.([]interface{})[0] == label {
		return true, nil
	} else {
		return false, nil
	}
}

func GetSession(method_id int64) neo4j.Session {
	session := Ndriver.NewSession(neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "prectice" + fmt.Sprint(method_id),
		FetchSize:    neo4j.FetchAll,
	})

	return session
}

func Get_type_influence_node(session neo4j.Session, guid_parent string, guid_node string) (bool, error) {

	node, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run("MATCH (p)-[s]->(n) WHERE p.guid = $guid_p AND n.guid = $guid_n RETURN s.typeOfEvidence", map[string]interface{}{
			"guid_p": guid_parent,
			"guid_n": guid_node,
		})
		if err != nil {
			return nil, err
		}
		if result.Next() {
			return result.Record().Values[0], nil
		}
		return nil, err
	})
	if err != nil {
		return true, err
	}
	//time.Sleep(time.Microsecond)
	if node.(string) == "True" {
		return true, nil
	} else {
		return false, nil
	}
}

func Get_degree_influence_node(session neo4j.Session, guid_parent string, guid_node string) (string, error) {

	influence, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run("MATCH (p)-[s]->(n) WHERE p.guid = $guid_p AND n.guid = $guid_n RETURN s.degreeOfEvidenceEnumValue", map[string]interface{}{
			"guid_p": guid_parent,
			"guid_n": guid_node,
		})
		if err != nil {
			return nil, err
		}
		if result.Next() {
			return result.Record().Values[0], nil
		}
		return nil, err
	})
	if err != nil {
		return "", err
	}
	//time.Sleep(time.Microsecond)
	return influence.(string), nil
}

func Get_normalValue_node(session neo4j.Session, guid string) (string, error) {

	normal, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run("MATCH (n) WHERE n.guid = $guid RETURN n.normalValue", map[string]interface{}{
			"guid": guid,
		})
		if err != nil {
			return nil, err
		}
		if result.Next() {
			return result.Record().Values[0], nil
		}
		return nil, err
	})
	if err != nil {
		return "", err
	}
	//time.Sleep(time.Microsecond)
	return normal.(string), nil
}

func Get_all_nodes_the_label(session neo4j.Session, label string) ([]string, error) {

	mas_name, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		var list []string
		request := "MATCH (n:" + label + ") RETURN n.guid"
		result, err := tx.Run(request, map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		for result.Next() {
			list = append(list, result.Record().Values[0].(string))
		}
		if err = result.Err(); err != nil {
			return nil, err
		}
		return list, nil
	})
	if err != nil {
		return nil, err
	}
	//time.Sleep(time.Microsecond)
	return mas_name.([]string), err
}
