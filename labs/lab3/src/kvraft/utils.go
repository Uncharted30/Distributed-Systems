package kvraft

func DeepCopyMap(m map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		result[k] = v
	}
	return result
}

func DeepCopySet(m map[string]EmptyStruct) map[string]EmptyStruct {
	result := make(map[string]EmptyStruct)
	for k, v := range m {
		result[k] = v
	}
	return result
}