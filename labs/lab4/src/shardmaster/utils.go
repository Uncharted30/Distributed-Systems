package shardmaster

func copyIntArray(arr [NShards]int) [NShards]int {
	res := [NShards]int{}
	for i, e := range arr {
		res[i] = e
	}
	return res
}

func copyStringArray(arr []string) []string {
	res := make([]string, 0)
	for _, e := range arr {
		res = append(res, e)
	}
	return res
}

func copyMap(source map[int][]string) map[int][]string {
	res := make(map[int][]string)
	for k, v := range source {
		res[k] = copyStringArray(v)
	}
	return res
}

func joinMap(first map[int][]string, second map[int][]string) {
	for k, v := range second {
		first[k] = v
	}
}
