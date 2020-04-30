package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

func doReduce(mapTasks int, taskId int, reducef func(string, []string) string) bool {
	out, err := os.Create("mr-out-" + strconv.Itoa(taskId))
	if err != nil {
		log.Printf("Reduce task %d error creating output file.", taskId)
		return false
	}

	var kva []KeyValue

	for i := 0; i < mapTasks; i++ {
		kva, _ = decodeJson(i, taskId, kva)
	}

	sort.Sort(ByKey(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		key := kva[i].Key
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[i].Value)
		}

		result := reducef(key, values)
		_, err := fmt.Fprintf(out, "%v %v\n", key, result)
		if err != nil {
			log.Printf("Reduce task %d rror output result.\n", taskId)
			return false
		}

		i = j
	}

	out.Close()
	return true
}

func decodeJson(index int, taskId int, kva []KeyValue) ([]KeyValue, bool) {
	filename := "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(taskId)
	file, err := os.Open(filename)

	if err != nil {
		log.Println("File " + filename + " does not exits.")
		return nil, false
	}

	dec := json.NewDecoder(file)

	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	return kva, true
}